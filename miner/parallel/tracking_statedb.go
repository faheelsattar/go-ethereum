package parallel

import (
	"maps"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

// trackingStateDB is a vm.StateDB that tracks the state an execution depends
// on and the state it writes, so its result can be validated and applied
// elsewhere.
type trackingStateDB struct {
	*state.StateDB
	reader   *versionedStateReader
	coinbase common.Address
	touched  map[key]struct{}
	written  map[key]struct{}
	fee      uint256.Int

	// Balance changes commute when the execution does not observe the
	// balance itself: adding to WETH's balance does not depend on it. Such
	// changes are kept as deltas, and a transfer guard only requires a minimum
	// balance, instead of making the exact balance a read.
	credited   map[common.Address]bool         // balances changed by AddBalance or SubBalance
	observed   map[common.Address]bool         // balances read exactly
	minBalance map[common.Address]*uint256.Int // balance required before the transaction
}

var _ vm.StateDB = (*trackingStateDB)(nil)

func newTrackingStateDB(sdb *state.StateDB, reader *versionedStateReader, coinbase common.Address) *trackingStateDB {
	return &trackingStateDB{
		StateDB:  sdb,
		reader:   reader,
		coinbase: coinbase,
		touched:  make(map[key]struct{}),
		written:  make(map[key]struct{}),

		credited:   make(map[common.Address]bool),
		observed:   make(map[common.Address]bool),
		minBalance: make(map[common.Address]*uint256.Int),
	}
}

func (t *trackingStateDB) touch(addr common.Address, fields ...field) {
	for _, f := range fields {
		t.touched[key{addr: addr, field: f}] = struct{}{}
	}
}

// write marks fields as written. A write also depends on the previous value,
// since a reverted write must publish exactly what was there before.
func (t *trackingStateDB) write(addr common.Address, fields ...field) {
	for _, f := range fields {
		t.touched[key{addr: addr, field: f}] = struct{}{}
		t.written[key{addr: addr, field: f}] = struct{}{}
	}
}

// touchAll marks every field: whether an account survives finalisation
// depends on all of them.
func (t *trackingStateDB) touchAll(addr common.Address) {
	t.touch(addr, exists, balance, nonce, code)
}

func (t *trackingStateDB) CreateAccount(addr common.Address) {
	t.write(addr, exists, balance, nonce, code)
	t.StateDB.CreateAccount(addr)
}

func (t *trackingStateDB) CreateContract(addr common.Address) {
	t.write(addr, exists, code)
	t.StateDB.CreateContract(addr)
}

func (t *trackingStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	t.changeBalance(addr, amount)
	return t.StateDB.SubBalance(addr, amount, reason)
}

func (t *trackingStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if addr == t.coinbase && !amount.IsZero() {
		// Credits to the coinbase commute, so they are applied as a delta
		// instead of depending on the balance they were added to.
		t.fee.Add(&t.fee, amount)
		return t.StateDB.AddBalance(addr, amount, reason)
	}
	t.changeBalance(addr, amount)
	return t.StateDB.AddBalance(addr, amount, reason)
}

// changeBalance records a balance change. A zero change only touches the
// account, which matters if the account is empty: then it gets deleted. Every
// zero-value call does this to its target, so for an account with code or a
// nonce, which is never empty, it depends on those alone. Any other change is a
// delta unless the balance gets observed.
func (t *trackingStateDB) changeBalance(addr common.Address, amount *uint256.Int) {
	if amount.IsZero() {
		t.touch(addr, exists, nonce, code)
		if t.mayBeEmpty(addr) {
			t.touchAll(addr)
			t.write(addr, exists, balance)
		}
		return
	}
	t.write(addr, exists)
	t.credited[addr] = true
}

// mayBeEmpty reports whether addr has neither a nonce nor code, so that its
// balance decides whether it is empty.
func (t *trackingStateDB) mayBeEmpty(addr common.Address) bool {
	hash := t.StateDB.GetCodeHash(addr)
	return t.StateDB.GetNonce(addr) == 0 && (hash == common.Hash{} || hash == types.EmptyCodeHash)
}

func (t *trackingStateDB) GetBalance(addr common.Address) *uint256.Int {
	t.touch(addr, exists, balance)
	t.observed[addr] = true
	return t.StateDB.GetBalance(addr)
}

// canTransfer is the EVM's transfer guard. Instead of reading the balance, it
// records the balance the account needs before the transaction for the guard
// to pass, given the transaction's own changes so far. A failing guard reads
// the balance exactly.
func (t *trackingStateDB) canTransfer(_ vm.StateDB, addr common.Address, amount *uint256.Int) bool {
	current := t.StateDB.GetBalance(addr)
	if current.Cmp(amount) < 0 {
		t.GetBalance(addr)
		return false
	}
	initial, err := t.reader.base(key{addr: addr, field: balance})
	if err != nil {
		t.GetBalance(addr)
		return true
	}
	// required = amount + initial - current, the initial balance that makes
	// the guard pass
	required := new(big.Int).Add(amount.ToBig(), initial.balance().ToBig())
	required.Sub(required, current.ToBig())
	if required.Sign() <= 0 {
		return true
	}
	need, overflow := uint256.FromBig(required)
	if overflow {
		t.GetBalance(addr)
		return true
	}
	if prev := t.minBalance[addr]; prev == nil || prev.Lt(need) {
		t.minBalance[addr] = need
	}
	return true
}

func (t *trackingStateDB) GetNonce(addr common.Address) uint64 {
	t.touch(addr, exists, nonce)
	return t.StateDB.GetNonce(addr)
}

func (t *trackingStateDB) SetNonce(addr common.Address, n uint64, reason tracing.NonceChangeReason) {
	t.write(addr, exists, nonce)
	t.StateDB.SetNonce(addr, n, reason)
}

func (t *trackingStateDB) GetCodeHash(addr common.Address) common.Hash {
	t.touch(addr, exists, code)
	return t.StateDB.GetCodeHash(addr)
}

func (t *trackingStateDB) GetCode(addr common.Address) []byte {
	t.touch(addr, exists, code)
	return t.StateDB.GetCode(addr)
}

func (t *trackingStateDB) GetCodeSize(addr common.Address) int {
	t.touch(addr, exists, code)
	return t.StateDB.GetCodeSize(addr)
}

func (t *trackingStateDB) SetCode(addr common.Address, c []byte, reason tracing.CodeChangeReason) []byte {
	t.write(addr, exists, code)
	return t.StateDB.SetCode(addr, c, reason)
}

func (t *trackingStateDB) GetState(addr common.Address, slot common.Hash) common.Hash {
	t.touched[key{addr: addr, field: storage, slot: slot}] = struct{}{}
	return t.StateDB.GetState(addr, slot)
}

func (t *trackingStateDB) GetStateAndCommittedState(addr common.Address, slot common.Hash) (common.Hash, common.Hash) {
	t.touched[key{addr: addr, field: storage, slot: slot}] = struct{}{}
	return t.StateDB.GetStateAndCommittedState(addr, slot)
}

func (t *trackingStateDB) SetState(addr common.Address, slot, val common.Hash) common.Hash {
	k := key{addr: addr, field: storage, slot: slot}
	t.touched[k] = struct{}{}
	t.written[k] = struct{}{}
	return t.StateDB.SetState(addr, slot, val)
}

func (t *trackingStateDB) SelfDestruct(addr common.Address) {
	t.write(addr, exists, balance, nonce, code)
	t.StateDB.SelfDestruct(addr)
}

func (t *trackingStateDB) Exist(addr common.Address) bool {
	t.touch(addr, exists)
	return t.StateDB.Exist(addr)
}

// Empty reads the balance only if the account may be empty: one with code or
// a nonce never is. Every value-carrying call asks this of its target.
func (t *trackingStateDB) Empty(addr common.Address) bool {
	t.touch(addr, exists, nonce, code)
	if t.mayBeEmpty(addr) {
		t.touch(addr, balance)
	}
	return t.StateDB.Empty(addr)
}

func (t *trackingStateDB) Touch(addr common.Address) {
	t.touchAll(addr)
	t.write(addr, exists)
	t.StateDB.Touch(addr)
}

// reads returns the values the execution observed for everything it touched.
func (t *trackingStateDB) reads() (map[key]value, error) {
	reads := make(map[key]value, len(t.touched))
	for k := range t.touched {
		v, err := t.reader.base(k)
		if err != nil {
			return nil, err
		}
		reads[k] = v
	}
	return reads, nil
}

// settleBalances decides, per credited account, whether its balance change is a
// delta or an exact write: observed, created, destroyed or emptied accounts
// fall back to exact. It returns the delta accounts.
func (t *trackingStateDB) settleBalances() map[common.Address]bool {
	deltas := make(map[common.Address]bool)
	for addr := range t.credited {
		_, exact := t.written[key{addr: addr, field: balance}]
		if exact || t.observed[addr] || !t.StateDB.Exist(addr) {
			t.write(addr, exists, balance)
			continue
		}
		deltas[addr] = true
	}
	for addr := range t.minBalance {
		if !deltas[addr] {
			t.touch(addr, exists, balance)
			delete(t.minBalance, addr)
		}
	}
	return deltas
}

// result captures the reads and the final writes of a finalised execution.
func (t *trackingStateDB) result() (*Result, error) {
	deltas := t.settleBalances()
	reads, err := t.reads()
	if err != nil {
		return nil, err
	}
	res := &Result{
		reads:     reads,
		writes:    make(map[key]value, len(t.written)),
		codes:     make(map[common.Hash][]byte),
		preimages: maps.Clone(t.StateDB.Preimages()),
	}
	deleted := make(map[common.Address]bool)
	for k := range t.written {
		if !t.StateDB.Exist(k.addr) {
			deleted[k.addr] = true
			continue
		}
		res.writes[k] = t.final(k, res.codes)
	}
	for addr := range deleted {
		res.writes[key{addr: addr, field: exists}] = boolValue(false)
		res.writes[key{addr: addr, field: balance}] = value{}
		res.writes[key{addr: addr, field: nonce}] = value{}
		res.writes[key{addr: addr, field: code}] = hashValue(types.EmptyCodeHash)
		res.writes[key{addr: addr, field: destructed}] = value{}
	}
	// The coinbase credit is its net balance change, not the sum of the
	// credits: a credit inside a call that reverted is rolled back in the
	// state but not in t.fee. If the coinbase is also a delta account, the
	// delta covers the credits too.
	if _, absolute := t.written[key{addr: t.coinbase, field: balance}]; !absolute && !t.fee.IsZero() && !deltas[t.coinbase] {
		initial, err := t.reader.base(key{addr: t.coinbase, field: balance})
		if err != nil {
			return nil, err
		}
		if final := t.StateDB.GetBalance(t.coinbase); final.Gt(initial.balance()) {
			res.fee = new(uint256.Int).Sub(final, initial.balance())
		}
	}
	for addr := range deltas {
		initial, err := t.reader.base(key{addr: addr, field: balance})
		if err != nil {
			return nil, err
		}
		d := new(big.Int).Sub(t.StateDB.GetBalance(addr).ToBig(), initial.balance().ToBig())
		if d.Sign() == 0 {
			continue
		}
		if res.deltas == nil {
			res.deltas = make(map[common.Address]*big.Int)
		}
		res.deltas[addr] = d
	}
	if len(t.minBalance) > 0 {
		res.minBalance = t.minBalance
	}
	return res, nil
}

// final returns the value of k after finalisation, registering new code.
func (t *trackingStateDB) final(k key, codes map[common.Hash][]byte) value {
	sdb := t.StateDB
	switch k.field {
	case exists:
		return boolValue(true)
	case balance:
		return balanceValue(sdb.GetBalance(k.addr))
	case nonce:
		return uintValue(sdb.GetNonce(k.addr))
	case code:
		hash := sdb.GetCodeHash(k.addr)
		if hash != types.EmptyCodeHash {
			codes[hash] = sdb.GetCode(k.addr)
		}
		return hashValue(hash)
	case storage:
		return hashValue(sdb.GetState(k.addr, k.slot))
	}
	return value{}
}
