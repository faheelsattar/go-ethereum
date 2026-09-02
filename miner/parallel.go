// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package miner

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/txorder"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

type parallelTxSource uint8

const (
	parallelPlainTx parallelTxSource = iota
	parallelBlobTx
)

// parallelTask is one transaction in Geth's selected ordering. Tasks may run
// on any worker, but a same-sender successor becomes runnable only after its
// predecessor has completed successfully.
type parallelTask struct {
	position     int
	source       parallelTxSource
	lazy         *txpool.LazyTransaction
	tx           *types.Transaction
	sender       common.Address
	chain        *parallelSenderChain
	previous     *parallelTask
	next         *parallelTask
	result       *parallelExecutionResult
	blobGasLimit bool
	blobGasLeft  uint64

	// Retry bookkeeping. settled tasks need no further processing; retried
	// tasks have been re-executed at least once; conflictKey remembers where
	// the last conflict happened so retry waves can chain contenders.
	settled     bool
	retried     bool
	conflictKey *parallelConflictLocation

	// What the latest execution could see, for validation: every result
	// committed before priorBase, plus every earlier link of the chain it ran
	// on (execChain/execIndex). Those writes are inputs, not conflicts.
	priorBase int
	execChain *parallelSenderChain
	execIndex int
}

// parallelSenderChain carries speculative state between nonce-dependent
// transactions. It is never owned by a worker and has at most one runnable
// task, so workers can safely hand it off through the shared queue.
type parallelSenderChain struct {
	state  *state.StateDB
	gas    *core.GasPool
	header *types.Header
}

type parallelExecutionResult struct {
	receipt    *types.Receipt
	bal        *bal.ConstructionBlockAccessList
	state      *state.ParallelStateResult
	gas        core.GasPoolDelta
	dependency *transactionAccess
	err        error
	duration   time.Duration
	stateCopy  time.Duration
}

// parallelResolveEntry is a future for one transaction. The first caller to
// request a hash performs the resolution and populates tx; concurrent callers
// for the same hash block on done, which is closed once tx is set.
type parallelResolveEntry struct {
	done chan struct{}
	tx   *types.Transaction
}

type parallelTransactionResolver struct {
	mu      sync.Mutex
	entries map[common.Hash]*parallelResolveEntry

	work         time.Duration
	blobWork     time.Duration
	wait         time.Duration // time callers spent blocked on another caller's in-flight resolution
	resolves     int
	blobResolves int
	cacheHits    int
}

func newParallelTransactionResolver() *parallelTransactionResolver {
	return &parallelTransactionResolver{entries: make(map[common.Hash]*parallelResolveEntry)}
}

// resolve returns the full transaction for lazy, resolving it at most once
// across all callers. Blob resolution can be expensive, so its cost is tracked separately.
func (r *parallelTransactionResolver) resolve(lazy *txpool.LazyTransaction, blob bool) *types.Transaction {
	r.mu.Lock()
	if entry, ok := r.entries[lazy.Hash]; ok {
		r.cacheHits++
		r.mu.Unlock()
		select {
		case <-entry.done:
		default:
			// The resolution is still in flight on another goroutine; record
			// how long this caller stalls on it.
			started := time.Now()
			<-entry.done
			waited := time.Since(started)
			r.mu.Lock()
			r.wait += waited
			r.mu.Unlock()
		}
		return entry.tx
	}
	entry := &parallelResolveEntry{done: make(chan struct{})}
	r.entries[lazy.Hash] = entry
	r.mu.Unlock()

	started := time.Now()
	entry.tx = lazy.Resolve()
	duration := time.Since(started)
	close(entry.done)

	r.mu.Lock()
	r.work += duration
	r.resolves++
	if blob {
		r.blobWork += duration
		r.blobResolves++
	}
	r.mu.Unlock()
	return entry.tx
}

// parallelBlobPrefetchWorkers bounds the goroutines constructing blob cell
// proofs, keeping that work off the evm execution workers.
const parallelBlobPrefetchWorkers = 4

func (r *parallelTransactionResolver) prefetchBlobTransactions(tasks []*parallelTask, interrupt *atomic.Int32) *sync.WaitGroup {
	var blobs []*parallelTask
	for _, task := range tasks {
		if task.source == parallelBlobTx && task.tx == nil && !task.blobGasLimit {
			blobs = append(blobs, task)
		}
	}
	wg := new(sync.WaitGroup)
	if len(blobs) == 0 {
		return wg
	}
	var next atomic.Int64
	for range min(parallelBlobPrefetchWorkers, len(blobs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if interrupt != nil && interrupt.Load() != commitInterruptNone {
					return
				}
				index := int(next.Add(1)) - 1
				if index >= len(blobs) {
					return
				}
				r.resolve(blobs[index].lazy, true)
			}
		}()
	}
	return wg
}

type parallelBuildMetrics struct {
	started          time.Time
	waves            int
	planned          int
	senderChains     int
	speculative      int
	merged           int
	firstAttempt     int
	retriedCommitted int
	conflicts        int
	retries          int
	retryRounds      int
	retryExecutions  int
	invalid          int
	maxActiveWorkers int
	executionWork    time.Duration
	executeWall      time.Duration // wall time of the parallel execution phases
	commitWall       time.Duration // wall time of the serial ordered-commit phases
	resolutionWork   time.Duration
	blobResolveWork  time.Duration
	resolveWait      time.Duration // time callers stalled on in-flight resolutions
	planningTime     time.Duration
	stateCopy        time.Duration
	mergeTime        time.Duration
	retryTime        time.Duration
	committed        int
	sequentialErrors int
	resolveCount     int
	blobResolveCount int
	resolveCacheHits int
	directConflicts  int
	senderChainStale int
	storageConflicts int
	balanceConflicts int
	nonceConflicts   int
	codeConflicts    int
	existConflicts   int
	incomplete       int
	conflictHotspots map[parallelConflictLocation]int
}

type parallelConflictLocation struct {
	kind    state.ParallelConflictKind
	address common.Address
	slot    common.Hash
	fields  state.ParallelAccountFields
}

type parallelConflict struct {
	previousHash     common.Hash
	previousPosition int
	state            *state.ParallelStateConflict
	incomplete       bool
}

func parallelDependencyBuildID(dependency *dependencyAnalyzer) uint64 {
	if dependency == nil {
		return 0
	}
	return dependency.buildID
}

func (miner *Miner) parallelWorkerCount() int {
	workers := miner.config.ParallelWorkers
	if workers <= 0 {
		workers = 8
	}
	return max(1, min(workers, runtime.GOMAXPROCS(0)))
}

func parallelExecutionSupported(env *environment) (bool, string) {
	if env.witness != nil {
		return false, "stateless-witness"
	}
	if env.state.Database().Type().Is(state.TypeUBT) {
		return false, "binary-trie-access-events"
	}
	return true, ""
}

func peekParallelTransaction(plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce) (parallelTxSource, *txpool.LazyTransaction) {
	source, tx, _ := peekParallelTransactionWithSender(plainTxs, blobTxs)
	return source, tx
}

func peekParallelTransactionWithSender(plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce) (parallelTxSource, *txpool.LazyTransaction, common.Address) {
	pltx, plfrom, ptip := plainTxs.PeekWithSender()
	bltx, blfrom, btip := blobTxs.PeekWithSender()
	switch {
	case pltx == nil:
		return parallelBlobTx, bltx, blfrom
	case bltx == nil:
		return parallelPlainTx, pltx, plfrom
	case ptip.Lt(btip):
		return parallelBlobTx, bltx, blfrom
	default:
		return parallelPlainTx, pltx, plfrom
	}
}

func transactionSource(source parallelTxSource, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce) *txorder.TransactionsByPriceAndNonce {
	if source == parallelBlobTx {
		return blobTxs
	}
	return plainTxs
}

// parallelPlanningGasFactor scales the remaining block gas into a planning
// budget of transaction gas limits. Transactions usually consume less than
// their limit, so plan more than one block's worth; anything beyond the
// budget waits for the next wave instead of being executed speculatively for
// nothing.
const parallelPlanningGasFactor = 2

// planParallelTasks consumes copies of Geth's ordering queues, leaving the
// originals untouched for ordered validation and commit. Planning stops once
// the cumulative gas limits of the planned transactions exceed gasBudget:
// the pending queues can hold far more gas than fits in the block, and every
// planned task costs a speculative execution.
func planParallelTasks(plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, availableBlobGas, gasBudget uint64) ([]*parallelTask, int) {
	plainCopy, blobCopy := plainTxs.Copy(), blobTxs.Copy()
	chains := make(map[common.Address]*parallelSenderChain)
	lastTaskBySender := make(map[common.Address]*parallelTask)
	var plannedBlobGas uint64
	var plannedGas uint64
	var tasks []*parallelTask

	for plannedGas < gasBudget {
		// peek transaction by price
		source, lazy, sender := peekParallelTransactionWithSender(plainCopy, blobCopy)
		if lazy == nil {
			break
		}
		plannedGas += lazy.Gas
		// we now need to select the queue from which we will be removing
		// the peeked transaction which we did above
		selectedQueue := transactionSource(source, plainCopy, blobCopy)
		if source == parallelBlobTx && lazy.BlobGas > availableBlobGas-plannedBlobGas {
			// Ordered commit will discard this sender and replan. Keep the
			// non-fitting transaction as a sentinel, but don't resolve it or
			// speculatively execute transactions that follow it.
			tasks = append(tasks, &parallelTask{
				position:     len(tasks),
				source:       source,
				lazy:         lazy,
				sender:       sender,
				blobGasLimit: true,
				blobGasLeft:  availableBlobGas - plannedBlobGas,
			})
			break
		}
		if source == parallelBlobTx {
			plannedBlobGas += lazy.BlobGas
		}
		chain := chains[sender]
		if chain == nil {
			chain = new(parallelSenderChain)
			chains[sender] = chain
		}
		task := &parallelTask{
			position:  len(tasks),
			source:    source,
			lazy:      lazy,
			sender:    sender,
			chain:     chain,
			execChain: chain,
		}
		// create the same sender nonce dependency links
		if previous := lastTaskBySender[sender]; previous != nil {
			// A0 -> A1 -> A2
			previous.next = task
			task.previous = previous
			task.execIndex = previous.execIndex + 1
		}
		lastTaskBySender[sender] = task
		tasks = append(tasks, task)

		// remove the peeked transaction from the copied queue
		selectedQueue.Shift()
	}
	return tasks, len(chains)
}

func (miner *Miner) executeParallelTask(env *environment, task *parallelTask, resolver *parallelTransactionResolver) {
	if task.result == nil {
		task.result = new(parallelExecutionResult)
	}
	result := task.result
	if task.tx == nil {
		tx := resolver.resolve(task.lazy, task.source == parallelBlobTx)
		if tx == nil {
			result.err = errors.New("transaction is no longer available")
			return
		}
		task.tx = tx
	}
	chain := task.chain
	if chain.state == nil {
		started := time.Now()
		// speculative view reads through env.state without copying it. This
		// requires env.state to stay unmodified while workers run, which holds
		// here: results are merged back only after the whole wave has finished.
		speculative, err := env.state.Speculative()
		if err != nil {
			result.err = err
			return
		}
		chain.state = speculative
		chain.gas = env.gasPool.Snapshot()
		chain.header = types.CopyHeader(env.header)
		result.stateCopy = time.Since(started)
	}

	var analyzer *dependencyAnalyzer
	vmConfig := vm.Config{}
	if env.dependency != nil {
		analyzer = newDependencyAnalyzer(0, chain.header, chain.state, "")
		analyzer.silent = true
		vmConfig.Tracer = analyzer.hooks()
	}
	evm := vm.NewEVM(core.NewEVMBlockContext(chain.header, miner.chain, &env.coinbase), chain.state, miner.chainConfig, vmConfig)
	defer evm.Release()

	index := env.tcount + task.position
	chain.state.SetTxContext(task.tx.Hash(), index, uint32(index+1))
	chain.state.StartParallelRecording()
	gasBefore := chain.gas.Snapshot()
	if analyzer != nil {
		analyzer.beginTransaction(task.tx, task.sender, index, 0)
	}
	started := time.Now()
	result.receipt, result.bal, result.err = core.ApplyTransaction(evm, chain.gas, chain.state, chain.header, task.tx)
	result.duration = time.Since(started)
	result.state = chain.state.FinishParallelRecording()
	if result.err != nil {
		if analyzer != nil {
			analyzer.abortTransaction()
		}
		return
	}
	result.gas, result.err = chain.gas.TransactionDelta(gasBefore)
	if result.err != nil {
		return
	}
	if analyzer != nil {
		analyzer.finishTransaction(result.receipt, result.duration)
		result.dependency = analyzer.transactions[len(analyzer.transactions)-1]
	}
	chain.header.GasUsed = chain.gas.Used()
}

// runParallelTask executes one speculative task, converting a worker panic
// into a failed, incomplete result. The speculative state is discarded and
// the commit loop re-executes the transaction sequentially on the canonical
// state, so a bug in the experimental executor degrades to sequential
// behavior instead of crashing the node.
func (miner *Miner) runParallelTask(env *environment, task *parallelTask, resolver *parallelTransactionResolver) {
	defer func() {
		if r := recover(); r != nil {
			if task.result == nil {
				task.result = new(parallelExecutionResult)
			}
			// clear the recorded state marks the result incomplete, which
			// routes the transaction into the sequential re exec path.
			task.result.state = nil
			task.result.err = fmt.Errorf("speculative execution panic: %v", r)
			log.Error("Optimistic transaction execution panicked",
				"hash", task.lazy.Hash,
				"sender", task.sender,
				"position", task.position,
				"err", r,
				"stack", string(debug.Stack()),
			)
		}
	}()
	miner.executeParallelTask(env, task, resolver)
}

func insertParallelReadyTask(ready []*parallelTask, task *parallelTask) []*parallelTask {
	index := sort.Search(len(ready), func(i int) bool {
		return ready[i].position > task.position
	})
	ready = append(ready, nil)
	copy(ready[index+1:], ready[index:])
	ready[index] = task
	return ready
}

// executeParallelTasks runs a work-conserving scheduler. Workers are generic:
// completing a task releases its same-sender successor back to the shared
// ready queue, where any worker may pick it up.
func (miner *Miner) executeParallelTasks(env *environment, tasks []*parallelTask, workers int, interrupt *atomic.Int32, resolver *parallelTransactionResolver, metrics *parallelBuildMetrics) bool {
	var ready []*parallelTask
	for _, task := range tasks {
		if task.chain != nil && task.previous == nil {
			ready = insertParallelReadyTask(ready, task)
		}
	}
	if len(ready) == 0 {
		return true
	}
	workers = min(workers, len(ready))
	jobs := make(chan *parallelTask)
	done := make(chan *parallelTask)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				miner.runParallelTask(env, task, resolver)
				done <- task
			}
		}()
	}

	active := 0
	interrupted := false
	for len(ready) > 0 || active > 0 {
		if interrupt != nil && interrupt.Load() != commitInterruptNone {
			interrupted = true
			ready = nil
		}
		var (
			out  chan *parallelTask
			next *parallelTask
		)
		if !interrupted && len(ready) > 0 {
			out, next = jobs, ready[0]
		}
		select {
		case out <- next:
			ready = ready[1:]
			active++
			metrics.maxActiveWorkers = max(metrics.maxActiveWorkers, active)
		case task := <-done:
			active--
			metrics.speculative++
			metrics.executionWork += task.result.duration
			metrics.stateCopy += task.result.stateCopy
			if task.result.err == nil && task.next != nil && !interrupted {
				ready = insertParallelReadyTask(ready, task.next)
			}
		}
	}
	close(jobs)
	wg.Wait()
	for _, task := range tasks {
		if task.chain != nil {
			task.chain.state = nil
			task.chain.gas = nil
			task.chain.header = nil
		}
	}
	return !interrupted
}

func parallelResultConflict(task *parallelTask, prior []parallelCommittedResult) *parallelConflict {
	if task.result == nil || task.result.state == nil {
		return &parallelConflict{incomplete: true}
	}
	for i := task.priorBase; i < len(prior); i++ {
		committed := prior[i]
		if committed.sender == task.sender {
			continue
		}
		// Writes of an earlier link of the task's own chain were visible to
		// its execution; they are inputs, not conflicts.
		if task.execChain != nil && committed.chain == task.execChain && committed.chainIndex < task.execIndex {
			continue
		}
		if conflict := task.result.state.Conflict(committed.state); conflict != nil {
			return &parallelConflict{
				previousHash:     committed.hash,
				previousPosition: committed.position,
				state:            conflict,
			}
		}
	}
	return nil
}

type parallelCommittedResult struct {
	hash       common.Hash
	position   int
	sender     common.Address
	state      *state.ParallelStateResult
	chain      *parallelSenderChain // chain the committed execution ran on, if any
	chainIndex int
}

func committedResult(task *parallelTask, result *state.ParallelStateResult) parallelCommittedResult {
	return parallelCommittedResult{
		hash:       task.tx.Hash(),
		position:   task.position,
		sender:     task.sender,
		state:      result,
		chain:      task.execChain,
		chainIndex: task.execIndex,
	}
}

func parallelAccountFieldsLabel(fields state.ParallelAccountFields) string {
	var labels []string
	if fields&state.ParallelAccountExistence != 0 {
		labels = append(labels, "existence")
	}
	if fields&state.ParallelAccountBalance != 0 {
		labels = append(labels, "balance")
	}
	if fields&state.ParallelAccountNonce != 0 {
		labels = append(labels, "nonce")
	}
	if fields&state.ParallelAccountCode != 0 {
		labels = append(labels, "code")
	}
	return strings.Join(labels, ",")
}

func conflictLocationOf(conflict *state.ParallelStateConflict) parallelConflictLocation {
	return parallelConflictLocation{
		kind:    conflict.Kind,
		address: conflict.Address,
		slot:    conflict.Slot,
		fields:  conflict.AccountFields,
	}
}

func (metrics *parallelBuildMetrics) recordDirectConflict(conflict *parallelConflict) {
	metrics.directConflicts++
	if conflict.incomplete || conflict.state == nil {
		metrics.incomplete++
		return
	}
	location := conflictLocationOf(conflict.state)
	if metrics.conflictHotspots == nil {
		metrics.conflictHotspots = make(map[parallelConflictLocation]int)
	}
	metrics.conflictHotspots[location]++
	if conflict.state.Kind == state.ParallelStorageConflict {
		metrics.storageConflicts++
		return
	}
	if conflict.state.AccountFields&state.ParallelAccountExistence != 0 {
		metrics.existConflicts++
	}
	if conflict.state.AccountFields&state.ParallelAccountBalance != 0 {
		metrics.balanceConflicts++
	}
	if conflict.state.AccountFields&state.ParallelAccountNonce != 0 {
		metrics.nonceConflicts++
	}
	if conflict.state.AccountFields&state.ParallelAccountCode != 0 {
		metrics.codeConflicts++
	}
}

func logParallelConflict(task *parallelTask, index int, conflict *parallelConflict) {
	ctx := []any{
		"hash", task.tx.Hash(),
		"sender", task.sender,
		"position", task.position,
		"index", index,
	}
	if conflict.incomplete || conflict.state == nil {
		log.Debug("Retrying optimistic transaction after incomplete speculative result", ctx...)
		return
	}
	ctx = append(ctx,
		"conflictsWith", conflict.previousHash,
		"conflictsWithPosition", conflict.previousPosition,
		"address", conflict.state.Address,
	)
	if conflict.state.Kind == state.ParallelStorageConflict {
		ctx = append(ctx, "reason", "storage-read-after-write", "slot", conflict.state.Slot)
	} else if conflict.state.AccountFields&state.ParallelAccountExistence != 0 {
		ctx = append(ctx, "reason", "account-existence-change", "accountFields", parallelAccountFieldsLabel(conflict.state.AccountFields))
	} else {
		ctx = append(ctx, "reason", "account-read-after-write", "accountFields", parallelAccountFieldsLabel(conflict.state.AccountFields))
	}
	log.Debug("Retrying optimistic transaction after stale state read", ctx...)
}

func topParallelConflictLocations(locations map[parallelConflictLocation]int, limit int) []string {
	type hotspot struct {
		count int
		label string
	}
	hotspots := make([]hotspot, 0, len(locations))
	for location, count := range locations {
		var label string
		if location.kind == state.ParallelStorageConflict {
			label = fmt.Sprintf("storage:%s:%s", location.address, location.slot)
		} else {
			label = fmt.Sprintf("account:%s:%s", location.address, parallelAccountFieldsLabel(location.fields))
		}
		hotspots = append(hotspots, hotspot{count: count, label: label})
	}
	sort.Slice(hotspots, func(i, j int) bool {
		if hotspots[i].count != hotspots[j].count {
			return hotspots[i].count > hotspots[j].count
		}
		return hotspots[i].label < hotspots[j].label
	})
	if len(hotspots) > limit {
		hotspots = hotspots[:limit]
	}
	result := make([]string, len(hotspots))
	for i, hotspot := range hotspots {
		result[i] = fmt.Sprintf("%s=%d", hotspot.label, hotspot.count)
	}
	return result
}

func (miner *Miner) mergeParallelTransaction(env *environment, task *parallelTask, candidateRank uint64) error {
	result := task.result
	if result == nil || result.err != nil || result.state == nil || result.receipt == nil {
		return errors.New("incomplete optimistic transaction result")
	}
	if err := env.gasPool.ApplyTransactionDelta(result.gas); err != nil {
		return err
	}
	blockHash := env.header.Hash()
	env.state.SetTxContext(task.tx.Hash(), env.tcount, uint32(env.tcount+1))
	env.state.ApplyParallelResult(result.state)

	result.receipt.CumulativeGasUsed = env.gasPool.CumulativeUsed()
	result.receipt.BlockHash = blockHash
	result.receipt.BlockNumber = env.header.Number
	result.receipt.TransactionIndex = uint(env.tcount)
	result.receipt.Logs = env.state.GetLogs(task.tx.Hash(), env.header.Number.Uint64(), blockHash, env.header.Time)
	result.receipt.Bloom = types.CreateBloom(result.receipt)
	env.header.GasUsed = env.gasPool.Used()

	if task.source == parallelBlobTx {
		sidecar := task.tx.BlobTxSidecar()
		if sidecar == nil {
			return errors.New("blob transaction without blobs")
		}
		tx := task.tx.WithoutBlobTxSidecar()
		env.txs = append(env.txs, tx)
		env.sidecars = append(env.sidecars, sidecar)
		env.blobs += len(sidecar.Blobs)
		env.size += tx.Size()
		*env.header.BlobGasUsed += result.receipt.BlobGasUsed
	} else {
		env.txs = append(env.txs, task.tx)
		env.size += task.tx.Size()
	}
	env.receipts = append(env.receipts, result.receipt)
	env.bal.Merge(result.bal)
	if env.dependency != nil {
		env.dependency.commitParallelTransaction(result.dependency, env.tcount, candidateRank)
	}
	env.tcount++
	return nil
}

// parallelMaxRetryRounds bounds how many parallel retry waves a block wave
// may run before the remaining conflicts fall back to serial re-execution on
// the commit goroutine, which guarantees termination.
const parallelMaxRetryRounds = 3

// parallelRetryChain tracks one retry chain while a retry wave is assembled.
type parallelRetryChain struct {
	chain *parallelSenderChain
	tail  *parallelTask
}

// buildParallelRetryWave rewires the stale tasks into a retry wave. Tasks are
// chained when they must (same sender: nonce order) or should (same conflict
// location: contenders on one slot re-execute serially within the wave,
// seeing each other's writes, so a hot slot converges in one round instead of
// one round per transaction). Independent chains run on separate workers.
// It returns the number of chains.
func buildParallelRetryWave(retry []*parallelTask) int {
	var (
		bySender   = make(map[common.Address]*parallelRetryChain)
		byLocation = make(map[parallelConflictLocation]*parallelRetryChain)
		chains     int
	)
	for _, task := range retry {
		task.result = nil
		task.retried = true
		task.previous, task.next = nil, nil

		// Nonce order makes the sender chain mandatory; the conflict-location
		// chain is a scheduling heuristic, so it loses ties.
		group := bySender[task.sender]
		if group == nil && task.conflictKey != nil {
			group = byLocation[*task.conflictKey]
		}
		if group == nil {
			group = &parallelRetryChain{chain: new(parallelSenderChain)}
			chains++
		}
		task.chain = group.chain
		task.execChain = group.chain
		task.execIndex = 0
		if group.tail != nil {
			group.tail.next = task
			task.previous = group.tail
			task.execIndex = group.tail.execIndex + 1
		}
		group.tail = task
		bySender[task.sender] = group
		if task.conflictKey != nil {
			byLocation[*task.conflictKey] = group
		}
	}
	return chains
}

// parallelWaveCommit carries the ordered-commit state of one wave across its
// retry rounds.
type parallelWaveCommit struct {
	prior         []parallelCommittedResult
	poppedSenders map[common.Address]bool
}

// commitTransactionsParallel takes the normal, and blob txs and determines how we can
// schedule the transactions on the configured workers, this is also checks for potential conflicts
// and re exec the transaction
func (miner *Miner) commitTransactionsParallel(ctx context.Context, env *environment, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, interrupt *atomic.Int32) error {
	metrics := &parallelBuildMetrics{started: time.Now()}
	workers := miner.parallelWorkerCount()
	resolver := newParallelTransactionResolver()
	defer func() {
		metrics.resolutionWork = resolver.work
		metrics.blobResolveWork = resolver.blobWork
		metrics.resolveWait = resolver.wait
		metrics.resolveCount = resolver.resolves
		metrics.blobResolveCount = resolver.blobResolves
		metrics.resolveCacheHits = resolver.cacheHits
		if env.benchmark != nil {
			env.benchmark.addParallelMetrics(metrics)
		}
		log.Info("Parallel block execution summary",
			"build", parallelDependencyBuildID(env.dependency),
			"number", env.header.Number,
			"workers", workers,
			"waves", metrics.waves,
			"planned", metrics.planned,
			"senderChains", metrics.senderChains,
			"maxActiveWorkers", metrics.maxActiveWorkers,
			"speculativeExecutions", metrics.speculative,
			"committed", metrics.committed,
			"merged", metrics.merged,
			"firstAttemptCommits", metrics.firstAttempt,
			"retriedCommits", metrics.retriedCommitted,
			"conflicts", metrics.conflicts,
			"directConflicts", metrics.directConflicts,
			"senderChainInvalidations", metrics.senderChainStale,
			"storageConflicts", metrics.storageConflicts,
			"balanceConflicts", metrics.balanceConflicts,
			"nonceConflicts", metrics.nonceConflicts,
			"codeConflicts", metrics.codeConflicts,
			"existenceConflicts", metrics.existConflicts,
			"incompleteConflicts", metrics.incomplete,
			"hotConflictLocations", topParallelConflictLocations(metrics.conflictHotspots, 5),
			"retries", metrics.retries,
			"retryRounds", metrics.retryRounds,
			"retryExecutions", metrics.retryExecutions,
			"invalid", metrics.invalid,
			"sequentialErrors", metrics.sequentialErrors,
			"wall", time.Since(metrics.started),
			"executeWall", metrics.executeWall,
			"commitWall", metrics.commitWall,
			"executionWork", metrics.executionWork,
			"resolutionWork", metrics.resolutionWork,
			"blobResolutionWork", metrics.blobResolveWork,
			"resolveWait", metrics.resolveWait,
			"resolveCount", metrics.resolveCount,
			"blobResolveCount", metrics.blobResolveCount,
			"resolveCacheHits", metrics.resolveCacheHits,
			"stateCopy", metrics.stateCopy,
			"mergeTime", metrics.mergeTime,
			"retryTime", metrics.retryTime,
		)
	}()

	for {
		if interrupt != nil {
			if signal := interrupt.Load(); signal != commitInterruptNone {
				return signalToErr(signal)
			}
		}
		if env.gasPool.Gas() < params.TxGas {
			break
		}
		// same check as in seq transaction processing
		// If we don't have enough blob space for any further blob transactions,
		// skip that list altogether
		if !blobTxs.Empty() && env.blobs >= miner.maxBlobsPerBlock(env.header.Time) {
			log.Trace("Not enough blob space for further blob transactions")
			blobTxs.Clear()
			// Fall though to pick up any plain txs
		}
		planningStarted := time.Now()
		availableBlobGas := ^uint64(0)
		if !blobTxs.Empty() {
			availableBlobs := miner.maxBlobsPerBlock(env.header.Time) - env.blobs
			availableBlobGas = uint64(availableBlobs) * params.BlobTxBlobGasPerBlob
		}
		// The wave must be sized before executing (no per-tx feedback like the
		// sequential path). Cap it at ~one blocks worth so planning doesnt walk
		// the whole pending pool executing transactions that cant fit, budgeted
		// in gas limits, which run ~2x actual usage. Leftovers go to the next wave.
		gasBudget := env.gasPool.Gas() * parallelPlanningGasFactor
		tasks, chains := planParallelTasks(plainTxs, blobTxs, availableBlobGas, gasBudget)
		metrics.planningTime += time.Since(planningStarted)
		if len(tasks) == 0 {
			break
		}
		metrics.waves++
		metrics.planned += len(tasks)
		metrics.senderChains = max(metrics.senderChains, chains)
		log.Debug("Executing optimistic transaction queue", "number", env.header.Number, "transactions", len(tasks), "senderChains", chains, "workers", workers)
		// Resolve blob sidecars on dedicated goroutines with a
		// a chance that it does not occupy the execution workers.
		// in worst case if worker out runs the prefetcher it will resolve
		// itself
		executeStarted := time.Now()
		blobPrefetch := resolver.prefetchBlobTransactions(tasks, interrupt)
		executed := miner.executeParallelTasks(env, tasks, workers, interrupt, resolver, metrics)
		blobPrefetch.Wait()
		metrics.executeWall += time.Since(executeStarted)
		if !executed {
			return signalToErr(interrupt.Load())
		}
		// Ordered commit with parallel retry rounds: each pass merges the
		// longest valid prefix in queue order, the stale tasks it collected
		// re-execute as a parallel retry wave against the advanced block
		// state, and the commit pass runs again. After parallelMaxRetryRounds
		// the remaining conflicts re-execute inline on the commit goroutine,
		// which always makes progress.
		wave := &parallelWaveCommit{poppedSenders: make(map[common.Address]bool)}
		var restart bool
		for round := 0; ; round++ {
			serialRetries := round >= parallelMaxRetryRounds
			commitStarted := time.Now()
			retry, r, done, err := miner.commitParallelTasks(ctx, env, tasks, plainTxs, blobTxs, wave, serialRetries, interrupt, metrics)
			metrics.commitWall += time.Since(commitStarted)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			restart = r
			if restart || len(retry) == 0 {
				break
			}
			metrics.retryRounds++
			metrics.retryExecutions += len(retry)
			buildParallelRetryWave(retry)
			// The retry executes against the block state as of now: every
			// result committed so far is an input, not a conflict candidate.
			for _, task := range retry {
				task.priorBase = len(wave.prior)
			}
			retryStarted := time.Now()
			executed := miner.executeParallelTasks(env, retry, workers, interrupt, resolver, metrics)
			metrics.executeWall += time.Since(retryStarted)
			if !executed {
				return signalToErr(interrupt.Load())
			}
		}
		if restart {
			continue
		}
	}
	return nil
}

type parallelTaskVerdict uint8

const (
	taskViable parallelTaskVerdict = iota
	taskNotViable
	taskBlockFull
)

// checkTaskViable applies the per-transaction inclusion checks the sequential
// builder also performs: blob budget, remaining gas, replay protection and
// block size.
func (miner *Miner) checkTaskViable(env *environment, task *parallelTask, candidateRank uint64) parallelTaskVerdict {
	if task.blobGasLimit {
		log.Trace("Not enough blob space left for transaction", "hash", task.lazy.Hash, "left", task.blobGasLeft/params.BlobTxBlobGasPerBlob, "needed", task.lazy.BlobGas/params.BlobTxBlobGasPerBlob)
		if env.dependency != nil {
			env.dependency.logCandidate(candidateRank, task.lazy.Hash, "skipped", "blob-gas-limit", nil)
		}
		return taskNotViable
	}
	if task.tx == nil {
		return taskNotViable
	}
	if env.gasPool.Gas() < task.lazy.Gas {
		return taskNotViable
	}
	if task.tx.Protected() && !miner.chainConfig.IsEIP155(env.header.Number) {
		return taskNotViable
	}
	if !env.txFitsSize(task.tx) {
		return taskBlockFull
	}
	if task.source == parallelBlobTx {
		if task.tx.BlobTxSidecar() == nil {
			return taskNotViable
		}
		if env.blobs+len(task.tx.BlobTxSidecar().Blobs) > miner.maxBlobsPerBlock(env.header.Time) {
			return taskNotViable
		}
	}
	return taskViable
}

// commitParallelTasks is the commit stage of the pipeline. It walks the
// wave's tasks in queue order and merges results until it finds a stale one:
// a result invalidated by a conflict, a broken sender chain, or a dropped
// input. From there it stops consuming the queue — preserving block order —
// and only collects the tasks already known stale, which the caller
// re-executes as the next retry wave. With serialRetries set (the terminal
// mode), stale tasks re-execute inline on the canonical state instead, which
// always finishes the wave. done reports that the block is full.
func (miner *Miner) commitParallelTasks(ctx context.Context, env *environment, tasks []*parallelTask, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, wave *parallelWaveCommit, serialRetries bool, interrupt *atomic.Int32, metrics *parallelBuildMetrics) (retry []*parallelTask, restart, done bool, err error) {
	var (
		merging = true
		// brokenSenders holds senders whose chain went stale mid-wave: their
		// later transactions must re-execute regardless of their results.
		brokenSenders = make(map[common.Address]*parallelTask)
		// droppedChains records, per execution chain, the lowest link whose
		// result did not commit in this pass. Later links of that chain
		// speculated on the dropped writes and must re-execute.
		droppedChains = make(map[*parallelSenderChain]int)
	)
	sawDroppedWrite := func(task *parallelTask) bool {
		index, ok := droppedChains[task.execChain]
		return ok && task.execIndex > index
	}
	drop := func(task *parallelTask) {
		if index, ok := droppedChains[task.execChain]; !ok || task.execIndex < index {
			droppedChains[task.execChain] = task.execIndex
		}
	}
	pop := func(task *parallelTask, ordered *txorder.TransactionsByPriceAndNonce) {
		ordered.Pop()
		wave.poppedSenders[task.sender] = true
		drop(task)
		metrics.invalid++
	}
	// collect queues a stale task for the next retry wave and stops the
	// merge: later tasks can only be gathered, never committed, so the block
	// order stays intact.
	collect := func(task *parallelTask, conflict *parallelConflict) {
		metrics.conflicts++
		task.conflictKey = nil
		if conflict != nil {
			metrics.recordDirectConflict(conflict)
			logParallelConflict(task, env.tcount, conflict)
			if conflict.state != nil && !conflict.incomplete {
				key := conflictLocationOf(conflict.state)
				task.conflictKey = &key
			}
		} else {
			metrics.senderChainStale++
		}
		brokenSenders[task.sender] = task
		drop(task)
		retry = append(retry, task)
		merging = false
	}

	for _, task := range tasks {
		if interrupt != nil {
			if signal := interrupt.Load(); signal != commitInterruptNone {
				return nil, false, false, signalToErr(signal)
			}
		}
		if task.settled {
			continue
		}
		if wave.poppedSenders[task.sender] {
			// Not in the queue anymore and never committing: anything that
			// speculated on its writes must re-execute.
			drop(task)
			continue
		}
		if !merging {
			// Gathering phase: collect what is already known stale, leave
			// everything else pending with its result intact.
			if brokenSenders[task.sender] != nil || sawDroppedWrite(task) {
				collect(task, nil)
				continue
			}
			if task.result == nil || task.result.err != nil || task.result.state == nil {
				continue // nothing to judge yet; a later pass handles it
			}
			if task.previous != nil && !task.previous.settled {
				continue // depends on a pending task; re-executing now would be premature
			}
			if conflict := parallelResultConflict(task, wave.prior); conflict != nil && conflict.state != nil && !conflict.incomplete {
				collect(task, conflict)
			}
			continue
		}

		// Merging phase: the task must match the queue head and still be
		// includable.
		source, current := peekParallelTransaction(plainTxs, blobTxs)
		if current == nil || source != task.source || current.Hash != task.lazy.Hash {
			log.Debug("Replanning optimistic transactions after ordering changed")
			return nil, true, false, nil
		}
		ordered := transactionSource(task.source, plainTxs, blobTxs)
		candidateRank := uint64(0)
		if env.dependency != nil {
			candidateRank = env.dependency.nextCandidate()
			env.candidateRank = candidateRank
		}
		switch miner.checkTaskViable(env, task, candidateRank) {
		case taskNotViable:
			pop(task, ordered)
			continue
		case taskBlockFull:
			return nil, false, true, nil
		}

		// Staleness check: a broken sender chain, a dropped input, or a
		// conflict with a result committed after this task executed.
		invalidating, senderBroken := brokenSenders[task.sender]
		droppedInput := sawDroppedWrite(task)
		var conflict *parallelConflict
		if !senderBroken && !droppedInput {
			conflict = parallelResultConflict(task, wave.prior)
		}
		if senderBroken || droppedInput || conflict != nil {
			if !serialRetries {
				collect(task, conflict)
				continue
			}
			miner.commitTaskSerially(ctx, env, task, ordered, wave, brokenSenders, drop, invalidating, conflict, metrics)
			continue
		}
		if task.result.err != nil {
			err := task.result.err
			log.Debug("Optimistic transaction is not executable", "hash", task.tx.Hash(), "sender", task.sender, "err", err)
			if errors.Is(err, core.ErrNonceTooLow) {
				// The pool state was stale; the sender's next transaction may
				// be valid, but its result rode on this one, so the chain
				// must re-execute.
				task.settled = true
				drop(task)
				ordered.Shift()
				brokenSenders[task.sender] = task
				if !serialRetries {
					merging = false
				}
				metrics.invalid++
			} else if !serialRetries && !task.retried {
				// The failure may be an artifact of stale speculative inputs;
				// give the transaction one fresh re-execution before dropping
				// its sender.
				collect(task, nil)
			} else {
				pop(task, ordered)
			}
			continue
		}
		mergeStart := time.Now()
		if err := miner.mergeParallelTransaction(env, task, candidateRank); err != nil {
			log.Debug("Optimistic result merge failed", "hash", task.tx.Hash(), "err", err)
			pop(task, ordered)
			continue
		}
		metrics.mergeTime += time.Since(mergeStart)
		metrics.merged++
		if task.retried {
			metrics.retriedCommitted++
		} else {
			metrics.firstAttempt++
		}
		metrics.committed++
		task.settled = true
		ordered.Shift()
		wave.prior = append(wave.prior, committedResult(task, task.result.state))
	}
	return retry, false, false, nil
}

// commitTaskSerially re-executes one stale transaction inline on the
// canonical block state. This is in commit order, so the fresh result cannot
// be stale and commits (or drops the sender) immediately.
func (miner *Miner) commitTaskSerially(ctx context.Context, env *environment, task *parallelTask, ordered *txorder.TransactionsByPriceAndNonce, wave *parallelWaveCommit, brokenSenders map[common.Address]*parallelTask, drop func(*parallelTask), invalidating *parallelTask, conflict *parallelConflict, metrics *parallelBuildMetrics) {
	metrics.conflicts++
	switch {
	case invalidating != nil:
		metrics.senderChainStale++
		log.Debug("Retrying optimistic transaction after sender chain invalidation",
			"hash", task.tx.Hash(), "sender", task.sender, "position", task.position, "index", env.tcount,
			"invalidatedBy", invalidating.tx.Hash(), "invalidatedByPosition", invalidating.position)
	case conflict != nil:
		metrics.recordDirectConflict(conflict)
		logParallelConflict(task, env.tcount, conflict)
		brokenSenders[task.sender] = task
	default: // a dropped input
		metrics.senderChainStale++
		log.Debug("Retrying optimistic transaction after observed result was dropped",
			"hash", task.tx.Hash(), "sender", task.sender, "position", task.position, "index", env.tcount)
		brokenSenders[task.sender] = task
	}
	started := time.Now()
	env.state.SetTxContext(task.tx.Hash(), env.tcount, uint32(env.tcount+1))
	env.state.StartParallelRecording()
	err := miner.commitTransaction(ctx, env, task.tx)
	sequentialState := env.state.FinishParallelRecording()
	metrics.retryTime += time.Since(started)
	metrics.retries++
	// Whatever happens, this task's speculative writes never commit: retry
	// chain members that observed them must re-execute.
	drop(task)
	if err != nil {
		metrics.sequentialErrors++
		if errors.Is(err, core.ErrNonceTooLow) {
			task.settled = true
			ordered.Shift()
		} else {
			ordered.Pop()
			wave.poppedSenders[task.sender] = true
			metrics.invalid++
		}
		return
	}
	task.settled = true
	ordered.Shift()
	metrics.committed++
	metrics.retriedCommitted++
	// The serial execution ran on the canonical state, not on the task's
	// chain, so the committed entry carries no chain identity: nothing may
	// skip validating against it.
	entry := committedResult(task, sequentialState)
	entry.chain, entry.chainIndex = nil, 0
	wave.prior = append(wave.prior, entry)
}
