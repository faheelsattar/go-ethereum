package parallel

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// blockStateReader serves the block under construction as a state.Reader, so
// a task can be executed on exactly the state it is committed to.
type blockStateReader struct {
	sdb *state.StateDB
}

var _ state.Reader = blockStateReader{}

func (r blockStateReader) Account(addr common.Address) (*types.StateAccount, error) {
	if !r.sdb.Exist(addr) {
		return nil, nil
	}
	return &types.StateAccount{
		Nonce:    r.sdb.GetNonce(addr),
		Balance:  r.sdb.GetBalance(addr).Clone(),
		Root:     types.EmptyRootHash,
		CodeHash: r.sdb.GetCodeHash(addr).Bytes(),
	}, nil
}

func (r blockStateReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	return r.sdb.GetState(addr, slot), nil
}

func (r blockStateReader) Code(addr common.Address, codeHash common.Hash) []byte {
	return r.sdb.GetCode(addr)
}

func (r blockStateReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	return r.sdb.GetCodeSize(addr)
}

func (r blockStateReader) Has(addr common.Address, codeHash common.Hash) bool {
	return r.sdb.GetCodeSize(addr) > 0
}
