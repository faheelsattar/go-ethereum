package parallel

import (
	"container/heap"
	"context"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// Verdict is a blocks decision about a candidate that is ready to commit.
type Verdict uint8

const (
	// Accept includes the candidate.
	Accept Verdict = iota
	// Reject drops the candidate and every later one from the same sender.
	Reject
	// Full stops building.
	Full
)

// Block is the block under construction, as seen by the committer.
type Block interface {
	// State is the block state that committed results are applied to.
	State() *state.StateDB
	// Check decides whether a candidate with a valid result still fits.
	Check(t *Task) (Verdict, error)
	// Include applies a validated result to the block, using Result.Apply.
	Include(t *Task, r *Result) error
}

// maxRequeues bounds how often a stale task is handed back to the workers
// before the committer executes it on the block state itself.
const maxRequeues = 2

// committer commits results as they arrive, lowest position first, without
// waiting for the positions in between. The block order is the commit order.
type committer struct {
	store     *store
	committed *store                              // writes of committed transactions, by commit order
	runOn     func(*Task, *state.StateDB) *Result // executes a task on the block state
	requeue   func(*Task)                         // hands a task back to the workers
	finished  <-chan *Task
	block     Block
	coinbase  common.Address
	dropped   map[common.Address]bool
	stats     *Stats
}

// run commits every task once its result has arrived and reports whether the
// block became full. It only waits while no finished task is left to commit.
func (c *committer) run(ctx context.Context, tasks []*Task) (bool, error) {
	var (
		ready   taskHeap
		handled = make(map[*Task]bool, len(tasks))
		parked  = make(map[*Task]*Task) // successors waiting for their same-sender predecessor
		lowest  = 0                     // index of the lowest task not handled yet
	)
	for len(handled) < len(tasks) {
		if len(ready) == 0 {
			started := time.Now()
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case t := <-c.finished:
				heap.Push(&ready, t)
			}
			waited := time.Since(started)
			c.stats.WaitTime += waited
			for handled[tasks[lowest]] {
				lowest++
			}
			if tasks[lowest].prev != nil {
				c.stats.ChainWaitTime += waited
			}
		}
		for more := true; more; {
			select {
			case t := <-c.finished:
				heap.Push(&ready, t)
			default:
				more = false
			}
		}
		t := heap.Pop(&ready).(*Task)
		// a requeued predecessor must commit first, or the nonce is off
		if t.prev != nil && !handled[t.prev] {
			parked[t.prev] = t
			continue
		}
		done, full, err := c.commit(t)
		if full || err != nil {
			return full, err
		}
		if !done {
			continue // requeued, it comes back on finished
		}
		handled[t] = true
		if next, ok := parked[t]; ok {
			delete(parked, t)
			heap.Push(&ready, next)
		}
	}
	return false, nil
}

// commit checks t against the block and includes it. A stale result is
// handed back to the workers, in which case t is not done yet.
func (c *committer) commit(t *Task) (done bool, full bool, err error) {
	if c.dropped[t.Sender] {
		c.drop(t)
		return true, false, nil
	}
	verdict, err := c.block.Check(t)
	if err != nil {
		return false, false, err
	}
	switch verdict {
	case Full:
		return false, true, nil
	case Reject:
		c.reject(t)
		return true, false, nil
	}
	t.mu.Lock()
	r := t.result
	t.mu.Unlock()
	started := time.Now()
	fresh := !stale(c.block.State(), r.reads)
	c.stats.ValidateTime += time.Since(started)
	if !fresh {
		c.stats.Stale++
		if t.requeues < maxRequeues {
			c.retract(t, r)
			t.requeues++
			c.stats.Requeued++
			c.requeue(t)
			return false, false, nil
		}
		started = time.Now()
		r = c.rerun(t, r)
		c.stats.WaitTime += time.Since(started)
	}
	if r.Err != nil {
		if errors.Is(r.Err, core.ErrNonceTooLow) {
			c.drop(t)
		} else {
			c.reject(t)
		}
		return true, false, nil
	}
	started = time.Now()
	err = c.block.Include(t, r)
	c.stats.CommitTime += time.Since(started)
	if err != nil {
		c.reject(t)
		return true, false, nil
	}
	c.publishCommitted(t.Position, r)
	c.stats.Committed++
	return true, false, nil
}

// retract removes the published writes of the stale result r of t.
func (c *committer) retract(t *Task, r *Result) {
	t.mu.Lock()
	c.store.unpublish(t.Position, r.writes)
	t.result = nil
	t.mu.Unlock()
}

// rerun replaces the result r of t by one computed on the block state, which
// cannot be stale, and publishes its writes for the positions above.
func (c *committer) rerun(t *Task, r *Result) *Result {
	c.retract(t, r)

	started := time.Now()
	r = c.runOn(t, c.block.State())
	c.stats.Executions++
	c.stats.ExecutionTime += time.Since(started)

	t.mu.Lock()
	c.store.publish(t.Position, r.writes, r.codes)
	t.result = r
	t.mu.Unlock()
	return r
}

func (c *committer) reject(t *Task) {
	c.dropped[t.Sender] = true
	c.drop(t)
}

func (c *committer) drop(t *Task) {
	t.mu.Lock()
	if t.result != nil {
		c.store.unpublish(t.Position, t.result.writes)
		t.result = nil
	}
	t.mu.Unlock()
	c.stats.Dropped++
}

// publishCommitted records the writes of the result r committed for the task
// at pos, in commit order, and the coinbase after it. Fees are applied as
// deltas, so this is where its true balance becomes visible to later tasks.
func (c *committer) publishCommitted(pos int, r *Result) {
	sdb := c.block.State()
	coinbase := map[key]value{
		{addr: c.coinbase, field: exists}:  boolValue(sdb.Exist(c.coinbase)),
		{addr: c.coinbase, field: balance}: balanceValue(sdb.GetBalance(c.coinbase)),
	}
	c.store.publish(pos, coinbase, nil)

	seq := c.stats.Committed
	c.committed.publish(seq, r.writes, r.codes)
	c.committed.publish(seq, coinbase, nil)
}

// stale reports whether any recorded read differs from the current state.
func stale(sdb *state.StateDB, reads map[key]value) bool {
	for k, v := range reads {
		switch k.field {
		case exists:
			if v != boolValue(sdb.Exist(k.addr)) {
				return true
			}
		case balance:
			if v != balanceValue(sdb.GetBalance(k.addr)) {
				return true
			}
		case nonce:
			if v != uintValue(sdb.GetNonce(k.addr)) {
				return true
			}
		case code:
			if !sdb.Exist(k.addr) {
				if v != hashValue(types.EmptyCodeHash) {
					return true
				}
			} else if v != hashValue(sdb.GetCodeHash(k.addr)) {
				return true
			}
		case storage:
			if v != hashValue(sdb.GetState(k.addr, k.slot)) {
				return true
			}
		}
	}
	return false
}
