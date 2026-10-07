package parallel

import (
	"context"
	"errors"
	"fmt"
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

// maxReruns bounds how often one task is re executed for stale reads.
const maxReruns = 3

// Block is the block under construction, as seen by the committer.
type Block interface {
	// State is the block state that committed results are applied to.
	State() *state.StateDB
	// Check decides whether a candidate with a valid result still fits.
	Check(t *Task) (Verdict, error)
	// Include applies a validated result to the block, using Result.Apply.
	Include(t *Task, r *Result) error
}

// committer goes through tasks in position order and commits each one as soon as
// it has a result whose reads still hold against the block state.
type committer struct {
	store    *store
	sched    *scheduler
	block    Block
	coinbase common.Address
	dropped  map[common.Address]bool
	stats    *Stats
}

// run commits tasks in order and reports whether the block became full.
// it waits for each task to finish, and re executes it if its reads no longer hold.
// note: in future maybe we can gain more speedup by not waiting for each task and
// not caring about the order of transactions in the block TBD.
func (c *committer) run(ctx context.Context, tasks []*Task) (bool, error) {
	for _, t := range tasks {
		if c.dropped[t.Sender] {
			c.drop(t)
			continue
		}
		started := time.Now()
		result, err := t.wait(ctx)
		c.stats.WaitTime += time.Since(started)
		if err != nil {
			return false, err
		}
		verdict, err := c.block.Check(t)
		if err != nil {
			return false, err
		}
		switch verdict {
		case Full:
			return true, nil
		case Reject:
			c.reject(t)
			continue
		}
		if result, err = c.settle(t, result); err != nil {
			return false, err
		}
		if result.Err != nil {
			if errors.Is(result.Err, core.ErrNonceTooLow) {
				c.drop(t)
			} else {
				c.reject(t)
			}
			continue
		}
		started = time.Now()
		err = c.block.Include(t, result)
		c.stats.CommitTime += time.Since(started)
		if err != nil {
			c.reject(t)
			continue
		}
		// tx was included so we can publish the coibase balance
		// to the store.
		c.publishCoinbase(t.Position)
		c.stats.Committed++
	}
	return false, nil
}

// settle reexecutes t until its results reads hold against the block state.
func (c *committer) settle(t *Task, r *Result) (*Result, error) {
	reruns := 0
	for {
		if r == nil {
			return nil, errors.New("task dropped while committing")
		}
		if !stale(c.block.State(), r.reads) {
			return r, nil
		}
		if reruns == maxReruns {
			return nil, fmt.Errorf("task %d still stale after %d reexecutions", t.Position, reruns)
		}
		// the result is stale, so discard it and execute the task again here
		// rather than wake a worker and wait for it.
		c.stats.Stale++
		started := time.Now()
		r = c.sched.rerun(t, r)
		c.stats.WaitTime += time.Since(started)
		reruns++
	}
}

func (c *committer) reject(t *Task) {
	c.dropped[t.Sender] = true
	c.drop(t)
}

func (c *committer) drop(t *Task) {
	t.mu.Lock()
	t.dropped = true
	if t.result != nil {
		c.store.unpublish(t.Position, t.result.writes)
		t.result = nil
	}
	t.mu.Unlock()
	c.stats.Dropped++
}

// publishCoinbase records the coinbase after a commit. Fees are applied as
// deltas, so this is where its true balance becomes visible to later tasks.
func (c *committer) publishCoinbase(pos int) {
	sdb := c.block.State()
	c.store.publish(pos, map[key]value{
		{addr: c.coinbase, field: exists}:  boolValue(sdb.Exist(c.coinbase)),
		{addr: c.coinbase, field: balance}: balanceValue(sdb.GetBalance(c.coinbase)),
	}, nil)
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
