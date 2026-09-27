package parallel

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/holiman/uint256"
)

// field selects one versioned part of an account.
type field uint8

const (
	exists field = iota
	balance
	nonce
	code
	storage
	// destructed marks the position at which the account was deleted. Its
	// storage reads as empty from there on.
	destructed
)

// key identifies one versioned piece of state.
type key struct {
	addr  common.Address
	field field
	slot  common.Hash // storage only
}

// value is the 32-byte encoding of a field value.
type value [32]byte

func boolValue(b bool) value {
	var v value
	if b {
		v[31] = 1
	}
	return v
}

func uintValue(n uint64) value {
	var v value
	binary.BigEndian.PutUint64(v[24:], n)
	return v
}

func hashValue(h common.Hash) value {
	return value(h)
}

func balanceValue(b *uint256.Int) value {
	return value(b.Bytes32())
}

func (v value) bool() bool {
	return v[31] != 0
}
func (v value) uint() uint64 {
	return binary.BigEndian.Uint64(v[24:])
}
func (v value) hash() common.Hash {
	return common.Hash(v)
}
func (v value) balance() *uint256.Int {
	return new(uint256.Int).SetBytes32(v[:])
}

// Task is one candidate transaction at a fixed position in the block order.
type Task struct {
	Position int
	Sender   common.Address
	Lazy     *txpool.LazyTransaction
	Blob     bool

	prev, next *Task // same sender neighbours; a scheduling hint only

	mu       sync.Mutex
	tx       *types.Transaction
	result   *Result
	done     chan struct{}
	dropped  bool
	released bool
}

// Follow records that t must not start before prev has executed once.
func (t *Task) Follow(prev *Task) {
	if prev == nil {
		return
	}
	t.prev = prev
	prev.next = t
}

// Tx returns the resolved transaction, or nil before the first execution.
func (t *Task) Tx() *types.Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tx
}

func (t *Task) resolve() *types.Transaction {
	if tx := t.Tx(); tx != nil {
		return tx
	}
	tx := t.Lazy.Resolve()
	t.mu.Lock()
	t.tx = tx
	t.mu.Unlock()
	return tx
}

func (t *Task) isDropped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dropped
}

// wait blocks until the current execution of t has finished
func (t *Task) wait(ctx context.Context) (*Result, error) {
	t.mu.Lock()
	done := t.done
	t.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result, nil
}

// Result is the outcome of one execution of a Task.
type Result struct {
	Receipt    *types.Receipt
	AccessList *bal.ConstructionBlockAccessList
	Gas        core.GasPoolDelta
	Err        error

	reads     map[key]value // values the execution observed
	writes    map[key]value // final values it produced
	codes     map[common.Hash][]byte
	preimages map[common.Hash][]byte
	fee       *uint256.Int // coinbase credit, applied additively
}

// accountWrites groups the writes of one result that target one account.
type accountWrites struct {
	deleted bool
	balance *value
	nonce   *uint64
	code    *common.Hash
	slots   map[common.Hash]common.Hash
}

// Apply writes the result into sdb and finalises the transaction. The caller
// must have set the transaction context on sdb.
func (r *Result) Apply(sdb *state.StateDB, coinbase common.Address) {
	byAddr := make(map[common.Address]*accountWrites)
	for k, v := range r.writes {
		w := byAddr[k.addr]
		if w == nil {
			w = &accountWrites{slots: make(map[common.Hash]common.Hash)}
			byAddr[k.addr] = w
		}
		switch k.field {
		case exists:
			w.deleted = !v.bool()
		case balance:
			b := v
			w.balance = &b
		case nonce:
			n := v.uint()
			w.nonce = &n
		case code:
			h := v.hash()
			w.code = &h
		case storage:
			w.slots[k.slot] = v.hash()
		}
	}
	addrs := slices.SortedFunc(func(yield func(common.Address) bool) {
		for a := range byAddr {
			if !yield(a) {
				return
			}
		}
	}, func(a, b common.Address) int { return bytes.Compare(a[:], b[:]) })

	for _, addr := range addrs {
		w := byAddr[addr]
		if w.deleted {
			sdb.SelfDestruct(addr)
			continue
		}
		if w.balance != nil {
			sdb.SetBalance(addr, w.balance.balance(), tracing.BalanceChangeUnspecified)
		}
		if w.nonce != nil {
			sdb.SetNonce(addr, *w.nonce, tracing.NonceChangeUnspecified)
		}
		if w.code != nil {
			sdb.SetCode(addr, r.codes[*w.code], tracing.CodeChangeUnspecified)
		}
		for _, slot := range slices.SortedFunc(func(yield func(common.Hash) bool) {
			for s := range w.slots {
				if !yield(s) {
					return
				}
			}
		}, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) }) {
			sdb.SetState(addr, slot, w.slots[slot])
		}
	}
	if r.fee != nil {
		sdb.AddBalance(coinbase, r.fee, tracing.BalanceIncreaseRewardTransactionFee)
	}
	if r.Receipt != nil {
		for _, l := range r.Receipt.Logs {
			cp := *l
			sdb.AddLog(&cp)
		}
	}
	for h, p := range r.preimages {
		sdb.AddPreimage(h, p)
	}
	sdb.Finalise(true)
}
