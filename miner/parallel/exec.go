package parallel

import (
	"errors"
	"fmt"
	"math"
	"runtime/debug"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

var errUnavailable = errors.New("transaction no longer available")

// noVersions lies below every position, so a reader at it serves only its parent.
const noVersions = math.MinInt

// executor runs tasks against the state seen from their position.
type executor struct {
	chain    core.ChainContext
	config   *params.ChainConfig
	db       state.Database
	root     common.Hash
	header   *types.Header
	coinbase common.Address
	store    *store
	parent   state.Reader
}

// run executes t on the parent state plus the versions below its position.
func (e *executor) run(t *Task) *Result {
	return e.execute(t, newVersionedStateReader(t.Position, e.store, e.parent))
}

// runOn executes t on the block state alone, as a sequential builder would.
func (e *executor) runOn(t *Task, sdb *state.StateDB) *Result {
	return e.execute(t, newVersionedStateReader(noVersions, e.store, blockStateReader{sdb}))
}

func (e *executor) execute(t *Task, reader *versionedStateReader) (res *Result) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("parallel transaction execution panicked", "hash", t.Lazy.Hash, "position", t.Position, "err", r, "stack", string(debug.Stack()))
			res = &Result{Err: fmt.Errorf("execution panic: %v", r)}
		}
	}()
	tx := t.resolve()
	if tx == nil {
		return &Result{Err: errUnavailable}
	}
	sdb, err := state.NewWithReader(e.root, e.db, reader)
	if err != nil {
		return &Result{Err: err}
	}
	trackedStateDB := newTrackingStateDB(sdb, reader, e.coinbase)
	evm := vm.NewEVM(core.NewEVMBlockContext(e.header, e.chain, &e.coinbase), trackedStateDB, e.config, vm.Config{})
	defer evm.Release()

	gas := core.NewGasPool(e.header.GasLimit)
	before := gas.Snapshot()
	sdb.SetTxContext(tx.Hash(), t.Position, uint32(t.Position+1))
	receipt, accessList, err := core.ApplyTransaction(evm, gas, sdb, e.header, tx)
	if err != nil {
		reads, readErr := trackedStateDB.reads()
		if readErr != nil {
			err = readErr
		}
		return &Result{Err: err, reads: reads}
	}
	if res, err = trackedStateDB.result(); err != nil {
		return &Result{Err: err}
	}
	if res.Gas, err = gas.TransactionDelta(before); err != nil {
		return &Result{Err: err}
	}
	res.Receipt, res.AccessList = receipt, accessList
	return res
}
