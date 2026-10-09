package parallel

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// SharedReader caches the parent state reads of a build for every reader of
// it: the workers' executions and the block state the committer applies
// results to. Workers usually read an account before the committer applies a
// result touching it, so the committer's loads become cache hits.
type SharedReader struct {
	state.Reader
	accounts sync.Map // common.Address -> *types.StateAccount, nil if absent
	slots    sync.Map // slotKey -> common.Hash
}

type slotKey struct {
	addr common.Address
	slot common.Hash
}

var _ state.Reader = (*SharedReader)(nil)

// NewSharedReader wraps reader, which must be safe for concurrent use.
func NewSharedReader(reader state.Reader) *SharedReader {
	return &SharedReader{Reader: reader}
}

// Account returns a copy of the cached account, loading it first if needed.
func (r *SharedReader) Account(addr common.Address) (*types.StateAccount, error) {
	if v, ok := r.accounts.Load(addr); ok {
		if acct := v.(*types.StateAccount); acct != nil {
			return acct.Copy(), nil
		}
		return nil, nil
	}
	acct, err := r.Reader.Account(addr)
	if err != nil {
		return nil, err
	}
	if acct == nil {
		r.accounts.Store(addr, (*types.StateAccount)(nil))
		return nil, nil
	}
	r.accounts.Store(addr, acct.Copy())
	return acct, nil
}

// Storage returns the cached slot, loading it first if needed.
func (r *SharedReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	k := slotKey{addr, slot}
	if v, ok := r.slots.Load(k); ok {
		return v.(common.Hash), nil
	}
	val, err := r.Reader.Storage(addr, slot)
	if err != nil {
		return common.Hash{}, err
	}
	r.slots.Store(k, val)
	return val, nil
}
