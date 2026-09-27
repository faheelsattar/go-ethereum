package parallel

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// versionedStateReader reads state as seen from one position: versions
// written by lower positions first, the parent state otherwise. It remembers
// what it served so the tracking StateDB can report the values an execution
// depended on.
type versionedStateReader struct {
	pos      int
	store    *store
	parent   state.Reader
	accounts map[common.Address]*types.StateAccount
	slots    map[key]common.Hash
}

var _ state.Reader = (*versionedStateReader)(nil)

func newVersionedStateReader(pos int, s *store, parent state.Reader) *versionedStateReader {
	return &versionedStateReader{
		pos:      pos,
		store:    s,
		parent:   parent,
		accounts: make(map[common.Address]*types.StateAccount),
		slots:    make(map[key]common.Hash),
	}
}

// Account retrieves the account associated with a particular address.
//
// - Returns a nil account if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned account is safe to modify after the call
func (r *versionedStateReader) Account(addr common.Address) (*types.StateAccount, error) {
	acct, ok := r.accounts[addr]
	if !ok {
		var err error
		if acct, err = r.loadAccount(addr); err != nil {
			return nil, err
		}
		r.accounts[addr] = acct
	}
	if acct == nil {
		return nil, nil
	}
	return acct.Copy(), nil
}

func (r *versionedStateReader) loadAccount(addr common.Address) (*types.StateAccount, error) {
	acct, err := r.parent.Account(addr)
	if err != nil {
		return nil, err
	}
	present := acct != nil
	if val, _, ok := r.store.read(key{addr: addr, field: exists}, r.pos); ok {
		present = val.bool()
	}
	if !present {
		return nil, nil
	}
	if acct == nil {
		acct = types.NewEmptyStateAccount()
	} else {
		acct = acct.Copy()
	}
	if val, _, ok := r.store.read(key{addr: addr, field: balance}, r.pos); ok {
		acct.Balance = val.balance()
	}
	if val, _, ok := r.store.read(key{addr: addr, field: nonce}, r.pos); ok {
		acct.Nonce = val.uint()
	}
	if val, _, ok := r.store.read(key{addr: addr, field: code}, r.pos); ok {
		acct.CodeHash = val.hash().Bytes()
	}
	return acct, nil
}

// Storage retrieves the storage slot associated with a particular account
// address and slot key.
//
// - Returns an empty slot if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned storage slot is safe to modify after the call
func (r *versionedStateReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	k := key{
		addr:  addr,
		field: storage,
		slot:  slot,
	}
	if val, ok := r.slots[k]; ok {
		return val, nil
	}
	val, err := r.loadSlot(k)
	if err != nil {
		return common.Hash{}, err
	}
	r.slots[k] = val
	return val, nil
}

func (r *versionedStateReader) loadSlot(k key) (common.Hash, error) {
	_, deletedAt, deleted := r.store.read(key{addr: k.addr, field: destructed}, r.pos)
	val, pos, ok := r.store.read(k, r.pos)
	switch {
	// slot exists and was written after the accounts storage was destructed
	// so it is the most recent value. This is for the case when the account
	// was deleted and then later was recereated and the slot was written again.
	// The slot is not destructed in this case.
	case ok && (!deleted || pos >= deletedAt):
		return val.hash(), nil
	// account was destructed, so the slot also donesnt exist
	case deleted:
		return common.Hash{}, nil
	}
	return r.parent.Storage(k.addr, k.slot)
}

// Code retrieves a particular contract's code. Returns nil code if the
// requested contract code doesn't exist.
func (r *versionedStateReader) Code(addr common.Address, codeHash common.Hash) []byte {
	if c, ok := r.store.code(codeHash); ok {
		return c
	}
	return r.parent.Code(addr, codeHash)
}

// CodeSize retrieves a particular contracts code's size. Returns zero code
// size if the requested contract code doesn't exist.
func (r *versionedStateReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	if c, ok := r.store.code(codeHash); ok {
		return len(c)
	}
	return r.parent.CodeSize(addr, codeHash)
}

// Has returns the flag indicating whether the contract code with
// specified address and hash exists or not.
func (r *versionedStateReader) Has(addr common.Address, codeHash common.Hash) bool {
	if _, ok := r.store.code(codeHash); ok {
		return true
	}
	return r.parent.Has(addr, codeHash)
}

// base returns the value this reader serves for k.
func (r *versionedStateReader) base(k key) (value, error) {
	checkAcctExists := func() (*types.StateAccount, error) {
		acct, err := r.Account(k.addr)
		if err != nil {
			return nil, err
		}
		return acct, nil
	}

	switch k.field {
	case storage:
		val, err := r.Storage(k.addr, k.slot)
		return hashValue(val), err
	case exists:
		acct, err := checkAcctExists()
		if err != nil {
			return value{}, err
		}
		return boolValue(acct != nil), nil
	case balance:
		acct, err := checkAcctExists()
		if err != nil || acct == nil {
			return value{}, err
		}
		return balanceValue(acct.Balance), nil
	case nonce:
		acct, err := checkAcctExists()
		if err != nil || acct == nil {
			return value{}, nil
		}
		return uintValue(acct.Nonce), nil
	case code:
		acct, err := checkAcctExists()
		if err != nil {
			return value{}, err
		}
		if acct == nil {
			return hashValue(types.EmptyCodeHash), nil
		}
		return hashValue(common.BytesToHash(acct.CodeHash)), nil
	}
	return value{}, nil
}
