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

// benchmarkRecorder appends one JSON line per benchmark event to a file.
type benchmarkRecorder struct {
	path string
	mu   sync.Mutex
}

// buildBenchmarkAttempt collects the measurements of one build iteration.
type buildBenchmarkAttempt struct {
	id        uint64
	payloadID engine.PayloadID
	iteration int
	mode      string
	strategy  string
	recorder  *benchmarkRecorder
	workers   int
	noBlobs   bool

	termination      string
	totalBuildWall   time.Duration // the whole build, including finalization
	transactionWall  time.Duration // the commit loops only, comparable across strategies
	executionTime    time.Duration // EVM time summed over all executions
	validateTime     time.Duration // parallel only: time the committer spent checking reads
	commitTime       time.Duration // parallel only: time the committer spent applying results
	waitTime         time.Duration // parallel only: time the committer spent waiting for results
	finalizationTime time.Duration

	batches       int
	planned       int
	executions    int
	stale         int
	committed     int
	dropped       int
	chained       int
	longestChain  int
	chainWaitTime time.Duration
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
	Blobs        int         `json:"blobs"`
	NoBlobs      bool        `json:"noBlobs,omitempty"`

	// Metric fields are always emitted: a zero is data, and conditionally
	// present fields make the JSONL painful to analyze.
	TotalBuildWallNs           int64 `json:"totalBuildWallNs"`
	TransactionExecutionWallNs int64 `json:"transactionExecutionWallNs"`
	EVMExecutionWorkNs         int64 `json:"evmExecutionWorkNs"`
	ValidateTimeNs             int64 `json:"validateTimeNs"`
	CommitTimeNs               int64 `json:"commitTimeNs"`
	WaitTimeNs                 int64 `json:"waitTimeNs"`
	FinalizationNs             int64 `json:"finalizationNs"`

	Workers    int `json:"workers"`
	Batches    int `json:"batches"`
	Planned    int `json:"planned"`
	Executions int `json:"executions"`
	Stale      int `json:"stale"`
	Committed  int `json:"committed"`
	Dropped    int `json:"dropped"`

	Chained         int   `json:"chained"`
	LongestChain    int   `json:"longestChain"`
	ChainWaitTimeNs int64 `json:"chainWaitTimeNs"`
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
	return &buildBenchmarkAttempt{
		id:        miner.benchmarkCounter.Add(1),
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
	attempt.batches += metrics.batches
	attempt.planned += metrics.Planned
	attempt.executions += metrics.Executions
	attempt.stale += metrics.Stale
	attempt.committed += metrics.Committed
	attempt.dropped += metrics.Dropped
	attempt.chained += metrics.Chained
	attempt.longestChain = max(attempt.longestChain, metrics.LongestChain)
	attempt.chainWaitTime += metrics.ChainWaitTime
	attempt.executionTime += metrics.ExecutionTime
	attempt.validateTime += metrics.ValidateTime
	attempt.commitTime += metrics.CommitTime
	attempt.waitTime += metrics.WaitTime
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
		TotalBuildWallNs:           attempt.totalBuildWall.Nanoseconds(),
		TransactionExecutionWallNs: attempt.transactionWall.Nanoseconds(),
		EVMExecutionWorkNs:         attempt.executionTime.Nanoseconds(),
		ValidateTimeNs:             attempt.validateTime.Nanoseconds(),
		CommitTimeNs:               attempt.commitTime.Nanoseconds(),
		WaitTimeNs:                 attempt.waitTime.Nanoseconds(),
		FinalizationNs:             attempt.finalizationTime.Nanoseconds(),
		Workers:                    attempt.workers,
		Batches:                    attempt.batches,
		Planned:                    attempt.planned,
		Executions:                 attempt.executions,
		Stale:                      attempt.stale,
		Committed:                  attempt.committed,
		Dropped:                    attempt.dropped,
		Chained:                    attempt.chained,
		LongestChain:               attempt.longestChain,
		ChainWaitTimeNs:            attempt.chainWaitTime.Nanoseconds(),
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
	attempt.totalBuildWall = elapsed
	event := attempt.event("build_completed")
	event.Completed = true
	event.Accepted = &accepted
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
	log.Info("Block building benchmark completed",
		"attempt", attempt.id,
		"id", attempt.payloadID,
		"iteration", attempt.iteration,
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
	accepted := true // a delivered payload is by definition the accepted best
	event.Accepted = &accepted
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
	log.Info("Block building benchmark payload delivered", "attempt", attempt.id, "id", attempt.payloadID, "strategy", attempt.strategy, "noBlobs", attempt.noBlobs, "number", event.BlockNumber, "transactions", event.Transactions)
}
