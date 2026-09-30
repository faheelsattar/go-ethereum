package parallel

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

type slotKey struct {
	addr common.Address
	slot common.Hash
}

type cachedStateReader struct {
	state.Reader

	mu       sync.RWMutex
	accounts map[common.Address]*types.StateAccount
	slots    map[slotKey]common.Hash
}

func newCachedStateReader(parent state.Reader) *cachedStateReader {
	return &cachedStateReader{
		Reader:   parent,
		accounts: make(map[common.Address]*types.StateAccount),
		slots:    make(map[slotKey]common.Hash),
	}
}

func (r *cachedStateReader) Account(addr common.Address) (*types.StateAccount, error) {
	r.mu.RLock()
	acct, ok := r.accounts[addr]
	r.mu.RUnlock()
	if !ok {
		var err error
		if acct, err = r.Reader.Account(addr); err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.accounts[addr] = acct
		r.mu.Unlock()
	}
	if acct == nil {
		return nil, nil
	}
	return acct.Copy(), nil
}

func (r *cachedStateReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	k := slotKey{addr, slot}
	r.mu.RLock()
	val, ok := r.slots[k]
	r.mu.RUnlock()
	if ok {
		return val, nil
	}
	val, err := r.Reader.Storage(addr, slot)
	if err != nil {
		return common.Hash{}, err
	}
	r.mu.Lock()
	r.slots[k] = val
	r.mu.Unlock()
	return val, nil
}
