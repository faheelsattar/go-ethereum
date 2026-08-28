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
		<-entry.done
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
const parallelBlobPrefetchWorkers = 2

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
	planned          int
	senderChains     int
	speculative      int
	merged           int
	firstAttempt     int
	retriedCommitted int
	conflicts        int
	retries          int
	invalid          int
	maxActiveWorkers int
	executionWork    time.Duration
	resolutionWork   time.Duration
	blobResolveWork  time.Duration
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
			position: len(tasks),
			source:   source,
			lazy:     lazy,
			sender:   sender,
			chain:    chain,
		}
		// create the same sender nonce dependency links
		if previous := lastTaskBySender[sender]; previous != nil {
			// A0 -> A1 -> A2
			previous.next = task
			task.previous = previous
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
	for _, committed := range prior {
		if committed.sender == task.sender {
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
	hash     common.Hash
	position int
	sender   common.Address
	state    *state.ParallelStateResult
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

func (metrics *parallelBuildMetrics) recordDirectConflict(conflict *parallelConflict) {
	metrics.directConflicts++
	if conflict.incomplete || conflict.state == nil {
		metrics.incomplete++
		return
	}
	location := parallelConflictLocation{
		kind:    conflict.state.Kind,
		address: conflict.state.Address,
		slot:    conflict.state.Slot,
		fields:  conflict.state.AccountFields,
	}
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
			"invalid", metrics.invalid,
			"sequentialErrors", metrics.sequentialErrors,
			"wall", time.Since(metrics.started),
			"executionWork", metrics.executionWork,
			"resolutionWork", metrics.resolutionWork,
			"blobResolutionWork", metrics.blobResolveWork,
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
		metrics.planned += len(tasks)
		metrics.senderChains = max(metrics.senderChains, chains)
		log.Debug("Executing optimistic transaction queue", "number", env.header.Number, "transactions", len(tasks), "senderChains", chains, "workers", workers)
		// Resolve blob sidecars on dedicated goroutines with a
		// a chance that it does not occupy the execution workers.
		// in worst case if worker out runs the prefetcher it will resolve
		// itself
		blobPrefetch := resolver.prefetchBlobTransactions(tasks, interrupt)
		executed := miner.executeParallelTasks(env, tasks, workers, interrupt, resolver, metrics)
		blobPrefetch.Wait()
		if !executed {
			return signalToErr(interrupt.Load())
		}

		var (
			prior         []parallelCommittedResult
			senderRetried = make(map[common.Address]*parallelTask)
			restart       bool
		)
		for _, task := range tasks {
			if interrupt != nil {
				if signal := interrupt.Load(); signal != commitInterruptNone {
					return signalToErr(signal)
				}
			}
			source, current := peekParallelTransaction(plainTxs, blobTxs)
			if current == nil || source != task.source || current.Hash != task.lazy.Hash {
				log.Debug("Replanning optimistic transactions after ordering changed")
				restart = true
				break
			}
			ordered := transactionSource(task.source, plainTxs, blobTxs)
			candidateRank := uint64(0)
			if env.dependency != nil {
				candidateRank = env.dependency.nextCandidate()
				env.candidateRank = candidateRank
			}
			if task.blobGasLimit {
				log.Trace("Not enough blob space left for transaction", "hash", task.lazy.Hash, "left", task.blobGasLeft/params.BlobTxBlobGasPerBlob, "needed", task.lazy.BlobGas/params.BlobTxBlobGasPerBlob)
				if env.dependency != nil {
					env.dependency.logCandidate(candidateRank, task.lazy.Hash, "skipped", "blob-gas-limit", nil)
				}
				ordered.Pop()
				metrics.invalid++
				restart = true
				break
			}
			if task.tx == nil {
				ordered.Pop()
				metrics.invalid++
				restart = true
				break
			}
			if env.gasPool.Gas() < task.lazy.Gas {
				ordered.Pop()
				metrics.invalid++
				restart = true
				break
			}
			if task.tx.Protected() && !miner.chainConfig.IsEIP155(env.header.Number) {
				ordered.Pop()
				metrics.invalid++
				restart = true
				break
			}
			if !env.txFitsSize(task.tx) {
				return nil
			}
			if task.source == parallelBlobTx {
				if task.tx.BlobTxSidecar() == nil {
					ordered.Pop()
					metrics.invalid++
					restart = true
					break
				}
				if env.blobs+len(task.tx.BlobTxSidecar().Blobs) > miner.maxBlobsPerBlock(env.header.Time) {
					ordered.Pop()
					metrics.invalid++
					restart = true
					break
				}
			}

			invalidatingTask, senderChainInvalidated := senderRetried[task.sender]
			var directConflict *parallelConflict
			if !senderChainInvalidated {
				directConflict = parallelResultConflict(task, prior)
			}
			conflict := senderChainInvalidated || directConflict != nil
			if conflict {
				metrics.conflicts++
				if senderChainInvalidated {
					metrics.senderChainStale++
					log.Debug("Retrying optimistic transaction after sender chain invalidation",
						"hash", task.tx.Hash(),
						"sender", task.sender,
						"position", task.position,
						"index", env.tcount,
						"invalidatedBy", invalidatingTask.tx.Hash(),
						"invalidatedByPosition", invalidatingTask.position,
						"reason", "sender-chain-invalidated",
					)
				} else {
					metrics.recordDirectConflict(directConflict)
					logParallelConflict(task, env.tcount, directConflict)
					senderRetried[task.sender] = task
				}
				started := time.Now()
				env.state.SetTxContext(task.tx.Hash(), env.tcount, uint32(env.tcount+1))
				env.state.StartParallelRecording()
				err := miner.commitTransaction(ctx, env, task.tx)
				sequentialState := env.state.FinishParallelRecording()
				metrics.retryTime += time.Since(started)
				metrics.retries++
				if err != nil {
					if errors.Is(err, core.ErrNonceTooLow) {
						ordered.Shift()
					} else {
						ordered.Pop()
					}
					metrics.sequentialErrors++
					restart = true
					break
				}
				ordered.Shift()
				metrics.committed++
				metrics.retriedCommitted++
				prior = append(prior, parallelCommittedResult{hash: task.tx.Hash(), position: task.position, sender: task.sender, state: sequentialState})
				continue
			}
			if task.result == nil || task.result.err != nil {
				err := errors.New("missing optimistic execution result")
				if task.result != nil && task.result.err != nil {
					err = task.result.err
				}
				if errors.Is(err, core.ErrNonceTooLow) {
					ordered.Shift()
				} else {
					ordered.Pop()
				}
				log.Debug("Optimistic transaction is not executable", "hash", task.tx.Hash(), "sender", task.sender, "err", err)
				metrics.invalid++
				restart = true
				break
			}
			mergeStart := time.Now()
			if err := miner.mergeParallelTransaction(env, task, candidateRank); err != nil {
				log.Debug("Optimistic result merge failed", "hash", task.tx.Hash(), "err", err)
				ordered.Pop()
				metrics.invalid++
				restart = true
				break
			}
			metrics.mergeTime += time.Since(mergeStart)
			metrics.merged++
			metrics.firstAttempt++
			metrics.committed++
			ordered.Shift()
			prior = append(prior, parallelCommittedResult{hash: task.tx.Hash(), position: task.position, sender: task.sender, state: task.result.state})
		}
		if restart {
			continue
		}
	}
	return nil
}
