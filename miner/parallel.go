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
	previous     *parallelTask
	next         *parallelTask
	result       *parallelExecutionResult
	blobGasLimit bool
	blobGasLeft  uint64

	// The chain this task runs on: its shared execution context and the
	// task's link index in it. Every earlier link's writes were visible to
	// this task's execution, so they are inputs, not conflicts.
	contextForChain *speculativeContextForChain
	indexInChain    int

	// Retry bookkeeping. settled tasks need no further processing; retried
	// tasks have been re-executed at least once; conflictKey remembers where
	// the last conflict happened so retry rounds can chain contenders.
	settled     bool
	retried     bool
	conflictKey *parallelConflictLocation

	// How many results had been committed to the block state when this
	// task's latest execution started. Those results were part of the state
	// it read, so they are inputs, not conflicts.
	commitsSeen int
}

// speculativeContextForChain is the execution context shared by a chain of
// tasks linked through parallelTask.previous/next: the speculative state each
// task runs on top of the previous one's writes, the gas pool that accumulates
// across them, and a private header copy since execution mutates GasUsed. At
// most one task on a chain is runnable at a time, so no worker ever contends
// for it.
type speculativeContextForChain struct {
	state  *state.StateDB
	gas    *core.GasPool
	header *types.Header
}

type parallelExecutionResult struct {
	receipt   *types.Receipt
	bal       *bal.ConstructionBlockAccessList
	state     *state.ParallelStateResult
	gas       core.GasPoolDelta
	err       error
	duration  time.Duration
	stateCopy time.Duration
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
	batches          int
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
// budget waits for the next batch instead of being executed speculatively for
// nothing.
const parallelPlanningGasFactor = 2

// planParallelTasks consumes copies of Geth's ordering queues, leaving the
// originals untouched for ordered validation and commit. Planning stops once
// the cumulative gas limits of the planned transactions exceed gasBudget:
// the pending queues can hold far more gas than fits in the block, and every
// planned task costs a speculative execution.
func planParallelTasks(plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, availableBlobGas, gasBudget uint64) ([]*parallelTask, int) {
	plainCopy, blobCopy := plainTxs.Copy(), blobTxs.Copy()
	chains := make(map[common.Address]*speculativeContextForChain)
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
			chain = new(speculativeContextForChain)
			chains[sender] = chain
		}
		task := &parallelTask{
			position:        len(tasks),
			source:          source,
			lazy:            lazy,
			sender:          sender,
			contextForChain: chain,
		}
		// create the same sender nonce dependency links
		if previous := lastTaskBySender[sender]; previous != nil {
			// A0 -> A1 -> A2
			previous.next = task
			task.previous = previous
			task.indexInChain = previous.indexInChain + 1
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
	chain := task.contextForChain
	if chain.state == nil {
		started := time.Now()
		// speculative view reads through env.state without copying it. This
		// requires env.state to stay unmodified while workers run, which holds
		// here: results are merged back only after the whole batch has finished.
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

	evm := vm.NewEVM(core.NewEVMBlockContext(chain.header, miner.chain, &env.coinbase), chain.state, miner.chainConfig, vm.Config{})
	defer evm.Release()

	index := env.tcount + task.position
	chain.state.SetTxContext(task.tx.Hash(), index, uint32(index+1))
	chain.state.StartParallelRecording()
	gasBefore := chain.gas.Snapshot()
	started := time.Now()
	result.receipt, result.bal, result.err = core.ApplyTransaction(evm, chain.gas, chain.state, chain.header, task.tx)
	result.duration = time.Since(started)
	result.state = chain.state.FinishParallelRecording()
	if result.err != nil {
		return
	}
	result.gas, result.err = chain.gas.TransactionDelta(gasBefore)
	if result.err != nil {
		return
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
	var readyTasks []*parallelTask
	for _, task := range tasks {
		if task.contextForChain != nil && task.previous == nil {
			readyTasks = insertParallelReadyTask(readyTasks, task)
		}
	}
	if len(readyTasks) == 0 {
		return true
	}
	workers = min(workers, len(readyTasks))
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
	for len(readyTasks) > 0 || active > 0 {
		if interrupt != nil && interrupt.Load() != commitInterruptNone {
			interrupted = true
			readyTasks = nil
		}
		var task *parallelTask
		if len(readyTasks) == 0 {
			// nothing to run rightnow, wait for a worker to finish.
			task = <-done
		} else {
			// hand  the lowest position ready task to the first free worker,
			// or collect a finished task if every worker is still busy.
			select {
			case jobs <- readyTasks[0]:
				readyTasks = readyTasks[1:]
				active++
				metrics.maxActiveWorkers = max(metrics.maxActiveWorkers, active)
				continue
			case task = <-done:
			}
		}
		// a task finshed, record it, and release its same sender successor
		// into the ready queue, where any free worker may pick it up.
		active--
		metrics.speculative++
		metrics.executionWork += task.result.duration
		metrics.stateCopy += task.result.stateCopy
		if task.result.err == nil && task.next != nil && !interrupted {
			readyTasks = insertParallelReadyTask(readyTasks, task.next)
		}
	}
	close(jobs)
	wg.Wait()
	for _, task := range tasks {
		if task.contextForChain != nil {
			task.contextForChain.state = nil
			task.contextForChain.gas = nil
			task.contextForChain.header = nil
		}
	}
	return !interrupted
}

func parallelResultConflict(task *parallelTask, committedResults []parallelCommittedResult) *parallelConflict {
	if task.result == nil || task.result.state == nil {
		return &parallelConflict{incomplete: true}
	}
	// Results committed before this execution ran were part of the state it
	// read, so only the ones committed after it can conflict.
	for i := task.commitsSeen; i < len(committedResults); i++ {
		committed := committedResults[i]
		if committed.sender == task.sender {
			continue
		}
		// Writes of an earlier link of the task's own chain were visible to
		// its execution; they are inputs, not conflicts.
		if task.contextForChain != nil && committed.chain == task.contextForChain && committed.chainIndex < task.indexInChain {
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
	chain      *speculativeContextForChain // chain the committed execution ran on, if any
	chainIndex int
}

func committedResult(task *parallelTask, result *state.ParallelStateResult) parallelCommittedResult {
	return parallelCommittedResult{
		hash:       task.tx.Hash(),
		position:   task.position,
		sender:     task.sender,
		state:      result,
		chain:      task.contextForChain,
		chainIndex: task.indexInChain,
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

func (miner *Miner) mergeParallelTransaction(env *environment, task *parallelTask) error {
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
	env.tcount++
	return nil
}

// parallelMaxRetryRounds bounds how many parallel retry rounds a batch
// may run before the remaining conflicts fall back to serial re-execution on
// the commit goroutine, which guarantees termination.
const parallelMaxRetryRounds = 3

// parallelRetryChain tracks one retry chain while a retry round is assembled.
type parallelRetryChain struct {
	chain *speculativeContextForChain
	tail  *parallelTask
}

// chainStaleTasks rewires the stale tasks into a retry round. Tasks are
// chained when they must (same sender: nonce order) or should (same conflict
// location: contenders on one slot re-execute serially within the batch,
// seeing each other's writes, so a hot slot converges in one round instead of
// one round per transaction). Independent chains run on separate workers.
// It returns the number of chains.
func chainStaleTasks(staleTasks []*parallelTask) int {
	var (
		bySender   = make(map[common.Address]*parallelRetryChain)
		byLocation = make(map[parallelConflictLocation]*parallelRetryChain)
		chains     int
	)
	for _, task := range staleTasks {
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
			group = &parallelRetryChain{chain: new(speculativeContextForChain)}
			chains++
		}
		task.contextForChain = group.chain
		task.indexInChain = 0
		if group.tail != nil {
			group.tail.next = task
			task.previous = group.tail
			task.indexInChain = group.tail.indexInChain + 1
		}
		group.tail = task
		bySender[task.sender] = group
		if task.conflictKey != nil {
			byLocation[*task.conflictKey] = group
		}
	}
	return chains
}

// parallelBatchCommit carries the ordered-commit state of one batch across its
// retry rounds.
type parallelBatchCommit struct {
	committed     []parallelCommittedResult // footprints of every merged result, in commit order
	poppedSenders map[common.Address]bool   // senders removed from the queues; their remaining tasks are skipped
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
		miner.lastParallelMetrics.Store(metrics)
		log.Info("Parallel block execution summary",
			"number", env.header.Number,
			"workers", workers,
			"batches", metrics.batches,
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
		// The batch must be sized before executing (no per-tx feedback like the
		// sequential path). Cap it at ~one blocks worth so planning doesnt walk
		// the whole pending pool executing transactions that cant fit, budgeted
		// in gas limits, which run ~2x actual usage. Leftovers go to the next batch.
		gasBudget := env.gasPool.Gas() * parallelPlanningGasFactor
		tasks, chains := planParallelTasks(plainTxs, blobTxs, availableBlobGas, gasBudget)
		metrics.planningTime += time.Since(planningStarted)
		if len(tasks) == 0 {
			break
		}
		metrics.batches++
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
		// Commit the batch in queue order, one round at a time. A round commits
		// tasks until it reaches the first one whose result is stale, then
		// returns that task and every later task already known to be stale.
		// Those are re-executed on the workers against the block state as it
		// now stands, and the next round tries to commit again. After
		// parallelMaxRetryRounds, stale tasks are instead re-executed one by one
		// on this goroutine as the walk reaches them, which always completes
		// the batch.
		batch := &parallelBatchCommit{poppedSenders: make(map[common.Address]bool)}
		var someTxOrderChanged bool
		for round := 0; ; round++ {
			inlineFallback := round >= parallelMaxRetryRounds
			commitStarted := time.Now()
			staleTasks, changed, blockFull, err := miner.commitParallelTasks(ctx, env, tasks, plainTxs, blobTxs, batch, inlineFallback, interrupt, metrics)
			metrics.commitWall += time.Since(commitStarted)
			if err != nil {
				return err
			}
			if blockFull {
				return nil
			}
			someTxOrderChanged = changed
			if someTxOrderChanged || len(staleTasks) == 0 {
				break
			}
			metrics.retryRounds++
			metrics.retryExecutions += len(staleTasks)
			chainStaleTasks(staleTasks)
			// The stale tasks re-execute against the block state as of now, so
			// every result committed so far is an input, not a conflict candidate.
			for _, task := range staleTasks {
				task.commitsSeen = len(batch.committed)
			}
			retryStarted := time.Now()
			executed := miner.executeParallelTasks(env, staleTasks, workers, interrupt, resolver, metrics)
			metrics.executeWall += time.Since(retryStarted)
			if !executed {
				return signalToErr(interrupt.Load())
			}
		}
		if someTxOrderChanged {
			// The real queue no longer matches the plan; plan a new batch from
			// wherever the queues now stand.
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
func (miner *Miner) checkTaskViable(env *environment, task *parallelTask) parallelTaskVerdict {
	if task.blobGasLimit {
		log.Trace("Not enough blob space left for transaction", "hash", task.lazy.Hash, "left", task.blobGasLeft/params.BlobTxBlobGasPerBlob, "needed", task.lazy.BlobGas/params.BlobTxBlobGasPerBlob)
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
// batch's tasks in queue order and merges results until it finds a stale one:
// a result invalidated by a conflict, a broken sender chain, or a dropped
// input. From there it stops consuming the queue — preserving block order —
// and only collects the tasks already known stale, which the caller
// re-executes as the next retry round. With inlineFallback set (the terminal
// mode), stale tasks re-execute inline on the canonical state instead, which
// always finishes the batch.
//
// It returns the stale tasks to re-execute, whether the real queue diverged
// from the plan (someTxOrderChanged: the caller must replan), and whether the block
// is full (blockFull: the caller must stop building).
func (miner *Miner) commitParallelTasks(ctx context.Context, env *environment, tasks []*parallelTask, plainTxs, blobTxs *txorder.TransactionsByPriceAndNonce, batch *parallelBatchCommit, inlineFallback bool, interrupt *atomic.Int32, metrics *parallelBuildMetrics) (stale []*parallelTask, someTxOrderChanged, blockFull bool, err error) {
	var (
		merging = true
		// brokenSenders holds senders whose chain went stale mid-batch: their
		// later transactions must re-execute regardless of their results.
		brokenSenders = make(map[common.Address]*parallelTask)
		// droppedChains records, per execution chain, the lowest link whose
		// result did not commit in this pass. Later links of that chain
		// speculated on the dropped writes and must re-execute.
		droppedChains = make(map[*speculativeContextForChain]int)
	)
	sawDroppedWrite := func(task *parallelTask) bool {
		index, ok := droppedChains[task.contextForChain]
		return ok && task.indexInChain > index
	}
	drop := func(task *parallelTask) {
		if index, ok := droppedChains[task.contextForChain]; !ok || task.indexInChain < index {
			droppedChains[task.contextForChain] = task.indexInChain
		}
	}
	pop := func(task *parallelTask, ordered *txorder.TransactionsByPriceAndNonce) {
		ordered.Pop()
		batch.poppedSenders[task.sender] = true
		drop(task)
		metrics.invalid++
	}
	// collect queues a stale task for the next retry round and stops the
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
		stale = append(stale, task)
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
		if batch.poppedSenders[task.sender] {
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
			if conflict := parallelResultConflict(task, batch.committed); conflict != nil && conflict.state != nil && !conflict.incomplete {
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
		switch miner.checkTaskViable(env, task) {
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
			conflict = parallelResultConflict(task, batch.committed)
		}
		if senderBroken || droppedInput || conflict != nil {
			if !inlineFallback {
				collect(task, conflict)
				continue
			}
			miner.commitTaskSerially(ctx, env, task, ordered, batch, brokenSenders, drop, invalidating, conflict, metrics)
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
				if !inlineFallback {
					merging = false
				}
				metrics.invalid++
			} else if !inlineFallback && !task.retried {
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
		if err := miner.mergeParallelTransaction(env, task); err != nil {
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
		batch.committed = append(batch.committed, committedResult(task, task.result.state))
	}
	return stale, false, false, nil
}

// commitTaskSerially re-executes one stale transaction inline on the
// canonical block state. This is in commit order, so the fresh result cannot
// be stale and commits (or drops the sender) immediately.
func (miner *Miner) commitTaskSerially(ctx context.Context, env *environment, task *parallelTask, ordered *txorder.TransactionsByPriceAndNonce, batch *parallelBatchCommit, brokenSenders map[common.Address]*parallelTask, drop func(*parallelTask), invalidating *parallelTask, conflict *parallelConflict, metrics *parallelBuildMetrics) {
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
			batch.poppedSenders[task.sender] = true
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
	batch.committed = append(batch.committed, entry)
}
