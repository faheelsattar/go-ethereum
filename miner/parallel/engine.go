package parallel

import (
	"context"
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
}

// Stats counts what happened during a build.
type Stats struct {
	Planned    int
	Executions int
	Stale      int
	Committed  int
	Dropped    int

	ExecutionTime time.Duration // time summed over all executions
	CommitTime    time.Duration // time the committer spent applying results
	WaitTime      time.Duration // time the committer spent waiting for results
}

// Engine executes candidate transactions in parallel and commits them in
// position order.
type Engine struct {
	store   *store
	exec    *executor
	workers int
	next    int
	dropped map[common.Address]bool
	stats   Stats
}

// New returns an engine building on the parent state described by cfg.
func New(cfg Config) (*Engine, error) {
	parent, err := cfg.DB.Reader(cfg.Root)
	if err != nil {
		return nil, err
	}
	s := newStore()
	return &Engine{
		store: s,
		exec: &executor{
			chain:    cfg.Chain,
			config:   cfg.Params,
			db:       cfg.DB,
			root:     cfg.Root,
			header:   types.CopyHeader(cfg.Header),
			coinbase: cfg.Coinbase,
			store:    s,
			parent:   parent,
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
		done:     make(chan struct{}),
	}
	e.next++
	e.stats.Planned++
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
	e.store.publish(-1, res.writes, res.codes)
	return nil
}

// Run executes tasks and commits them into block in position order. It returns
// true once block reports that it is full.
func (e *Engine) Run(ctx context.Context, tasks []*Task, block Block) (bool, error) {
	sched := newScheduler(e.store, e.workers, e.exec.run)
	sched.start(tasks)
	defer func() {
		executions, executionTime := sched.stop()
		e.stats.Executions += executions
		e.stats.ExecutionTime += executionTime
	}()

	c := &committer{
		store:    e.store,
		sched:    sched,
		block:    block,
		coinbase: e.exec.coinbase,
		dropped:  e.dropped,
		stats:    &e.stats,
	}
	return c.run(ctx, tasks)
}

func (e *Engine) Stats() Stats {
	return e.stats
}
