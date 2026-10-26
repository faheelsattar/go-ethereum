package parallel

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type Config struct {
	Workers  int
	Chain    core.ChainContext
	Params   *params.ChainConfig
	DB       state.Database
	Root     common.Hash // parent state root
	Header   *types.Header
	Coinbase common.Address

	// Reader, if set, reads the parent state instead of DB.Reader(Root), such
	// as a SharedReader the block state also reads through.
	Reader state.Reader
	// JumpDests and Precompiles, if set, are the chain's caches of code
	// analysis and precompile results, which the sequential builder uses too.
	JumpDests   vm.JumpDestCache
	Precompiles *vm.PrecompileCache
}

// Stats counts what happened during a build.
type Stats struct {
	Planned      int
	Chained      int // tasks that had to wait for an earlier one from the same sender
	LongestChain int
	Executions   int
	Stale        int // results whose reads no longer held at commit time
	Requeued     int // stale tasks handed back to the workers
	Committed    int
	Dropped      int

	ExecutionTime time.Duration // time summed over all executions
	ValidateTime  time.Duration // time the committer spent checking reads against the block state
	CommitTime    time.Duration // time the committer spent applying results
	WaitTime      time.Duration // time the committer spent waiting for results
	ChainWaitTime time.Duration // the part of WaitTime spent with a chained task as the lowest one left

	// StaleBy counts stale results by the location of the read that failed
	// first: "<address>" for account fields, "<address>/storage".
	StaleBy map[string]int
}

func (s *Stats) recordStale(k key) {
	if s.StaleBy == nil {
		s.StaleBy = make(map[string]int)
	}
	loc := k.addr.Hex()
	if k.field == storage {
		loc += "/storage"
	} else if k.field == balance {
		loc += "/balance"
	}
	s.StaleBy[loc]++
}

// Engine executes candidate transactions in parallel and commits them in
// position order.
type Engine struct {
	store     *store
	committed *store // writes of committed transactions, versioned by commit order
	exec      *executor
	workers   int
	next      int
	dropped   map[common.Address]bool
	stats     Stats
}

// New returns an engine building on the parent state described by cfg.
func New(cfg Config) (*Engine, error) {
	parent := cfg.Reader
	if parent == nil {
		var err error
		if parent, err = cfg.DB.Reader(cfg.Root); err != nil {
			return nil, err
		}
	}
	// a sequential build reuses one EVM and its code analysis
	jumpDests := cfg.JumpDests
	if jumpDests == nil {
		jumpDests = core.NewJumpDestCache()
	}
	s, committed := newStore(), newStore()
	return &Engine{
		store:     s,
		committed: committed,
		exec: &executor{
			chain:       cfg.Chain,
			config:      cfg.Params,
			db:          cfg.DB,
			root:        cfg.Root,
			header:      types.CopyHeader(cfg.Header),
			coinbase:    cfg.Coinbase,
			store:       s,
			committed:   committed,
			parent:      parent,
			jumpDests:   jumpDests,
			precompiles: cfg.Precompiles,
		},
		workers: max(1, cfg.Workers),
		dropped: make(map[common.Address]bool),
	}, nil
}

// NewTask registers a candidate at the next position.
func (e *Engine) NewTask(lazy *txpool.LazyTransaction, sender common.Address, blob bool) *Task {
	t := &Task{
		Position: e.next,
		Sender:   sender,
		Lazy:     lazy,
		Blob:     blob,
	}
	e.next++
	e.stats.Planned++
	// resolving a blob transaction reconstructs its blobs, which takes
	// milliseconds, so start right away instead of on the worker
	if blob {
		go t.resolve()
	}
	return t
}

// Dropped reports whether the senders remaining candidates are excluded.
func (e *Engine) Dropped(sender common.Address) bool {
	return e.dropped[sender]
}

// Seed runs fn on the parent state and makes its writes visible to every
// task, so executions see the changes made before the first transaction
// parent writes are written at position -1 in the store
func (e *Engine) Seed(fn func(vm.StateDB) error) error {
	reader := newVersionedStateReader(0, e.store, e.exec.parent)
	sdb, err := state.NewWithReader(e.exec.root, e.exec.db, reader)
	if err != nil {
		return err
	}
	tracked := newTrackingStateDB(sdb, reader, e.exec.coinbase)
	if err := fn(tracked); err != nil {
		return err
	}
	res, err := tracked.result()
	if err != nil {
		return err
	}
	// additive balance changes become absolute versions, as the committer
	// publishes them for transactions
	writes := res.writes
	if len(res.deltas) > 0 || res.fee != nil {
		writes = maps.Clone(res.writes)
		credited := slices.Collect(maps.Keys(res.deltas))
		if res.fee != nil {
			credited = append(credited, e.exec.coinbase)
		}
		for _, addr := range credited {
			writes[key{addr: addr, field: exists}] = boolValue(true)
			writes[key{addr: addr, field: balance}] = balanceValue(sdb.GetBalance(addr))
		}
	}
	e.store.publish(-1, writes, res.codes)
	e.committed.publish(-1, writes, res.codes)
	return nil
}

// Run executes tasks and commits them into block as their results arrive. It
// returns true once block reports that it is full.
func (e *Engine) Run(ctx context.Context, tasks []*Task, block Block) (bool, error) {
	chain := make(map[*Task]int, len(tasks))
	for _, t := range tasks {
		chain[t] = 1
		if t.prev != nil {
			chain[t] = chain[t.prev] + 1
			e.stats.Chained++
		}
		e.stats.LongestChain = max(e.stats.LongestChain, chain[t])
	}
	sched := newScheduler(e.store, e.workers, e.exec.run)
	sched.start(tasks)
	defer func() {
		executions, executionTime := sched.stop()
		e.stats.Executions += executions
		e.stats.ExecutionTime += executionTime
	}()

	c := &committer{
		store:     e.store,
		committed: e.committed,
		runOn:     e.exec.runOn,
		requeue:   sched.push,
		finished:  sched.finished,
		block:     block,
		coinbase:  e.exec.coinbase,
		dropped:   e.dropped,
		stats:     &e.stats,
	}
	return c.run(ctx, tasks)
}

func (e *Engine) Stats() Stats {
	return e.stats
}
