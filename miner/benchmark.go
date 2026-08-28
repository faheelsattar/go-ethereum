package miner

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

const (
	parallelBenchmarkOff       = "off"
	parallelBenchmarkAlternate = "alternate"

	benchmarkStrategyParallel   = "parallel"
	benchmarkStrategySequential = "sequential"
)

type benchmarkRecorder struct {
	path string
	mu   sync.Mutex
}

type buildBenchmarkAttempt struct {
	id        uint64
	payloadID engine.PayloadID
	iteration int
	mode      string
	strategy  string
	recorder  *benchmarkRecorder
	workers   int
	noBlobs   bool

	transactionWall    time.Duration
	totalBuildWall     time.Duration
	evmExecutionWork   time.Duration
	resolutionWork     time.Duration
	blobResolutionWork time.Duration
	stateCopy          time.Duration
	mergeTime          time.Duration
	retryTime          time.Duration
	planningTime       time.Duration
	finalizationTime   time.Duration
	termination        string

	planned          int
	senderChains     int
	speculative      int
	committed        int
	merged           int
	firstAttempt     int
	retriedCommitted int
	conflicts        int
	directConflicts  int
	senderChainStale int
	storageConflicts int
	balanceConflicts int
	nonceConflicts   int
	codeConflicts    int
	existConflicts   int
	incomplete       int
	retries          int
	invalid          int
	sequentialErrors int
	maxActiveWorkers int
	resolveCount     int
	blobResolveCount int
	resolveCacheHits int

}

type benchmarkEvent struct {
	Event       string    `json:"event"`
	Timestamp   time.Time `json:"timestamp"`
	Attempt     uint64    `json:"attempt"`
	PayloadID   string    `json:"payloadId"`
	Iteration   int       `json:"iteration"`
	Mode        string    `json:"mode"`
	Strategy    string    `json:"strategy"`
	Completed   bool      `json:"completed"`
	Accepted    *bool     `json:"accepted,omitempty"`
	Delivered   bool      `json:"delivered,omitempty"`
	Termination string    `json:"termination,omitempty"`
	Error       string    `json:"error,omitempty"`

	BlockNumber  uint64      `json:"blockNumber,omitempty"`
	BlockHash    common.Hash `json:"blockHash,omitempty"`
	ParentHash   common.Hash `json:"parentHash,omitempty"`
	StateRoot    common.Hash `json:"stateRoot,omitempty"`
	ReceiptsRoot common.Hash `json:"receiptsRoot,omitempty"`
	Transactions int         `json:"transactions,omitempty"`
	GasUsed      uint64      `json:"gasUsed,omitempty"`
	BlockSize    uint64      `json:"blockSize,omitempty"`
	Blobs        int         `json:"blobs,omitempty"`
	NoBlobs      bool        `json:"noBlobs,omitempty"`

	// Metric fields are always emitted (no omitempty): a zero is data, and
	// conditionally present fields make the JSONL painful to analyze.
	TotalBuildWallNs           int64 `json:"totalBuildWallNs"`
	TransactionExecutionWallNs int64 `json:"transactionExecutionWallNs"`
	EVMExecutionWorkNs         int64 `json:"evmExecutionWorkNs"`
	ResolutionWorkNs           int64 `json:"resolutionWorkNs"`
	BlobResolutionWorkNs       int64 `json:"blobResolutionWorkNs"`
	PlanningNs                 int64 `json:"planningNs"`
	StateCopyNs                int64 `json:"stateCopyNs"`
	MergeNs                    int64 `json:"mergeNs"`
	RetryNs                    int64 `json:"retryNs"`
	FinalizationNs             int64 `json:"finalizationNs"`

	Workers                  int `json:"workers"`
	Planned                  int `json:"planned"`
	SenderChains             int `json:"senderChains"`
	MaxActiveWorkers         int `json:"maxActiveWorkers"`
	SpeculativeExecutions    int `json:"speculativeExecutions"`
	Committed                int `json:"committed"`
	Merged                   int `json:"merged"`
	FirstAttemptCommits      int `json:"firstAttemptCommits"`
	RetriedCommits           int `json:"retriedCommits"`
	Conflicts                int `json:"conflicts"`
	DirectConflicts          int `json:"directConflicts"`
	SenderChainInvalidations int `json:"senderChainInvalidations"`
	StorageConflicts         int `json:"storageConflicts"`
	BalanceConflicts         int `json:"balanceConflicts"`
	NonceConflicts           int `json:"nonceConflicts"`
	CodeConflicts            int `json:"codeConflicts"`
	ExistenceConflicts       int `json:"existenceConflicts"`
	IncompleteConflicts      int `json:"incompleteConflicts"`
	Retries                  int `json:"retries"`
	Invalid                  int `json:"invalid"`
	SequentialErrors         int `json:"sequentialErrors"`
	ResolveCount             int `json:"resolveCount"`
	BlobResolveCount         int `json:"blobResolveCount"`
	ResolveCacheHits         int `json:"resolveCacheHits"`

}

func newBenchmarkRecorder(path string) *benchmarkRecorder {
	if path == "" {
		return nil
	}
	return &benchmarkRecorder{path: path}
}

func (r *benchmarkRecorder) write(event benchmarkEvent) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	file, err := os.OpenFile(r.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		log.Warn("Failed to open parallel benchmark output", "path", r.path, "err", err)
		return
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(event); err != nil {
		log.Warn("Failed to write parallel benchmark output", "path", r.path, "err", err)
	}
}

// payloadBenchmarkStrategy assigns the benchmark strategy for one payload.
// The strategy is fixed per payload rather than per build iteration.
func (miner *Miner) payloadBenchmarkStrategy() string {
	if miner.config.ParallelBenchmarkMode != parallelBenchmarkAlternate {
		return benchmarkStrategyParallel
	}
	// ABBA over payloads: sequential, parallel, parallel, sequential.
	switch (miner.benchmarkPayloadCounter.Add(1) - 1) % 4 {
	case 0, 3:
		return benchmarkStrategySequential
	default:
		return benchmarkStrategyParallel
	}
}

func (miner *Miner) newBenchmarkAttempt(payloadID engine.PayloadID, iteration int, strategy string) *buildBenchmarkAttempt {
	mode := miner.config.ParallelBenchmarkMode
	if mode != parallelBenchmarkAlternate {
		return nil
	}
	id := miner.benchmarkCounter.Add(1)
	return &buildBenchmarkAttempt{
		id:        id,
		payloadID: payloadID,
		iteration: iteration,
		mode:      mode,
		strategy:  strategy,
		recorder:  miner.benchmarkRecorder,
		workers:   miner.parallelWorkerCount(),
		noBlobs:   miner.config.ParallelBenchmarkNoBlobs,
	}
}

func (attempt *buildBenchmarkAttempt) parallel() bool {
	return attempt != nil && attempt.strategy == benchmarkStrategyParallel
}

func (attempt *buildBenchmarkAttempt) addParallelMetrics(metrics *parallelBuildMetrics) {
	if attempt == nil {
		return
	}
	attempt.planned += metrics.planned
	attempt.senderChains = max(attempt.senderChains, metrics.senderChains)
	attempt.speculative += metrics.speculative
	attempt.committed += metrics.committed
	attempt.merged += metrics.merged
	attempt.firstAttempt += metrics.firstAttempt
	attempt.retriedCommitted += metrics.retriedCommitted
	attempt.conflicts += metrics.conflicts
	attempt.directConflicts += metrics.directConflicts
	attempt.senderChainStale += metrics.senderChainStale
	attempt.storageConflicts += metrics.storageConflicts
	attempt.balanceConflicts += metrics.balanceConflicts
	attempt.nonceConflicts += metrics.nonceConflicts
	attempt.codeConflicts += metrics.codeConflicts
	attempt.existConflicts += metrics.existConflicts
	attempt.incomplete += metrics.incomplete
	attempt.retries += metrics.retries
	attempt.invalid += metrics.invalid
	attempt.sequentialErrors += metrics.sequentialErrors
	attempt.maxActiveWorkers = max(attempt.maxActiveWorkers, metrics.maxActiveWorkers)
	attempt.evmExecutionWork += metrics.executionWork
	attempt.resolutionWork += metrics.resolutionWork
	attempt.blobResolutionWork += metrics.blobResolveWork
	attempt.stateCopy += metrics.stateCopy
	attempt.mergeTime += metrics.mergeTime
	attempt.retryTime += metrics.retryTime
	attempt.planningTime += metrics.planningTime
	attempt.resolveCount += metrics.resolveCount
	attempt.blobResolveCount += metrics.blobResolveCount
	attempt.resolveCacheHits += metrics.resolveCacheHits
}

func (attempt *buildBenchmarkAttempt) event(name string) benchmarkEvent {
	return benchmarkEvent{
		Event:                      name,
		Timestamp:                  time.Now().UTC(),
		Attempt:                    attempt.id,
		PayloadID:                  attempt.payloadID.String(),
		Iteration:                  attempt.iteration,
		Mode:                       attempt.mode,
		Strategy:                   attempt.strategy,
		Termination:                attempt.termination,
		NoBlobs:                    attempt.noBlobs,
		TransactionExecutionWallNs: attempt.transactionWall.Nanoseconds(),
		TotalBuildWallNs:           attempt.totalBuildWall.Nanoseconds(),
		EVMExecutionWorkNs:         attempt.evmExecutionWork.Nanoseconds(),
		ResolutionWorkNs:           attempt.resolutionWork.Nanoseconds(),
		BlobResolutionWorkNs:       attempt.blobResolutionWork.Nanoseconds(),
		PlanningNs:                 attempt.planningTime.Nanoseconds(),
		StateCopyNs:                attempt.stateCopy.Nanoseconds(),
		MergeNs:                    attempt.mergeTime.Nanoseconds(),
		RetryNs:                    attempt.retryTime.Nanoseconds(),
		FinalizationNs:             attempt.finalizationTime.Nanoseconds(),
		Workers:                    attempt.workers,
		Planned:                    attempt.planned,
		SenderChains:               attempt.senderChains,
		MaxActiveWorkers:           attempt.maxActiveWorkers,
		SpeculativeExecutions:      attempt.speculative,
		Committed:                  attempt.committed,
		Merged:                     attempt.merged,
		FirstAttemptCommits:        attempt.firstAttempt,
		RetriedCommits:             attempt.retriedCommitted,
		Conflicts:                  attempt.conflicts,
		DirectConflicts:            attempt.directConflicts,
		SenderChainInvalidations:   attempt.senderChainStale,
		StorageConflicts:           attempt.storageConflicts,
		BalanceConflicts:           attempt.balanceConflicts,
		NonceConflicts:             attempt.nonceConflicts,
		CodeConflicts:              attempt.codeConflicts,
		ExistenceConflicts:         attempt.existConflicts,
		IncompleteConflicts:        attempt.incomplete,
		Retries:                    attempt.retries,
		Invalid:                    attempt.invalid,
		SequentialErrors:           attempt.sequentialErrors,
		ResolveCount:               attempt.resolveCount,
		BlobResolveCount:           attempt.blobResolveCount,
		ResolveCacheHits:           attempt.resolveCacheHits,
	}
}

func countBlockBlobs(block *types.Block) int {
	var blobs int
	for _, tx := range block.Transactions() {
		blobs += len(tx.BlobHashes())
	}
	return blobs
}

func (attempt *buildBenchmarkAttempt) recordCompleted(result *newPayloadResult, elapsed time.Duration, accepted bool) {
	event := attempt.event("build_completed")
	event.Completed = true
	event.Accepted = &accepted
	attempt.totalBuildWall = elapsed
	event.TotalBuildWallNs = elapsed.Nanoseconds()
	event.BlockNumber = result.block.NumberU64()
	event.BlockHash = result.block.Hash()
	event.ParentHash = result.block.ParentHash()
	event.StateRoot = result.block.Root()
	event.ReceiptsRoot = result.block.ReceiptHash()
	event.Transactions = len(result.block.Transactions())
	event.GasUsed = result.block.GasUsed()
	event.BlockSize = result.block.Size()
	event.Blobs = countBlockBlobs(result.block)
	attempt.recorder.write(event)
	log.Info("Block execution benchmark completed",
		"attempt", attempt.id,
		"id", attempt.payloadID,
		"iteration", attempt.iteration,
		"mode", attempt.mode,
		"strategy", attempt.strategy,
		"noBlobs", attempt.noBlobs,
		"number", event.BlockNumber,
		"transactions", event.Transactions,
		"gasUsed", event.GasUsed,
		"transactionWall", attempt.transactionWall,
		"totalBuildWall", elapsed,
		"accepted", accepted,
		"termination", attempt.termination,
	)
}

func (attempt *buildBenchmarkAttempt) recordInterrupted(elapsed time.Duration, err error) {
	event := attempt.event("build_interrupted")
	event.TotalBuildWallNs = elapsed.Nanoseconds()
	if err != nil {
		event.Error = err.Error()
	}
	attempt.recorder.write(event)
}

func (attempt *buildBenchmarkAttempt) recordDelivered(block *types.Block) {
	event := attempt.event("payload_delivered")
	event.Completed = true
	event.Delivered = true
	event.BlockNumber = block.NumberU64()
	event.BlockHash = block.Hash()
	event.ParentHash = block.ParentHash()
	event.StateRoot = block.Root()
	event.ReceiptsRoot = block.ReceiptHash()
	event.Transactions = len(block.Transactions())
	event.GasUsed = block.GasUsed()
	event.BlockSize = block.Size()
	event.Blobs = countBlockBlobs(block)
	attempt.recorder.write(event)
	log.Info("Block execution benchmark payload delivered", "attempt", attempt.id, "id", attempt.payloadID, "strategy", attempt.strategy, "noBlobs", attempt.noBlobs, "number", event.BlockNumber, "transactions", event.Transactions)
}
