// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/legacypool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

var (
	// Test chain configurations
	testTxPoolConfig  legacypool.Config
	ethashChainConfig *params.ChainConfig
	cliqueChainConfig *params.ChainConfig

	// Test accounts
	testBankKey, _  = crypto.GenerateKey()
	testBankAddress = crypto.PubkeyToAddress(testBankKey.PublicKey)
	testBankFunds   = big.NewInt(1000000000000000000)

	testUserKey, _  = crypto.GenerateKey()
	testUserAddress = crypto.PubkeyToAddress(testUserKey.PublicKey)
	testContract    = common.HexToAddress("0x0100000000000000000000000000000000000000")

	testCounterContract = common.HexToAddress("0x0200000000000000000000000000000000000000")
	testConflictKeys    = newTestKeys(4)

	// Test transactions
	pendingTxs []*types.Transaction
	newTxs     []*types.Transaction

	testConfig = Config{
		PendingFeeRecipient: testBankAddress,
		Recommit:            time.Second,
		GasCeil:             params.GenesisGasLimit,
	}
)

func init() {
	testTxPoolConfig = legacypool.DefaultConfig
	testTxPoolConfig.Journal = ""
	ethashChainConfig = new(params.ChainConfig)
	*ethashChainConfig = *params.TestChainConfig
	cliqueChainConfig = new(params.ChainConfig)
	*cliqueChainConfig = *params.TestChainConfig
	cliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}

	signer := types.LatestSigner(params.TestChainConfig)
	tx1 := types.MustSignNewTx(testBankKey, signer, &types.AccessListTx{
		ChainID:  params.TestChainConfig.ChainID,
		Nonce:    0,
		To:       &testUserAddress,
		Value:    big.NewInt(1000),
		Gas:      params.TxGas,
		GasPrice: big.NewInt(params.InitialBaseFee),
	})
	pendingTxs = append(pendingTxs, tx1)

	tx2 := types.MustSignNewTx(testBankKey, signer, &types.LegacyTx{
		Nonce:    1,
		To:       &testUserAddress,
		Value:    big.NewInt(1000),
		Gas:      params.TxGas,
		GasPrice: big.NewInt(params.InitialBaseFee),
	})
	newTxs = append(newTxs, tx2)
}

// testWorkerBackend implements worker.Backend interfaces and wraps all information needed during the testing.
type testWorkerBackend struct {
	db      ethdb.Database
	txPool  *txpool.TxPool
	chain   *core.BlockChain
	genesis *core.Genesis
}

func newTestKeys(n int) []*ecdsa.PrivateKey {
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
	}
	return keys
}

func counterCalls(signer types.Signer, keys []*ecdsa.PrivateKey, n uint64) []*types.Transaction {
	var txs []*types.Transaction
	for _, key := range keys {
		for nonce := uint64(0); nonce < n; nonce++ {
			txs = append(txs, types.MustSignNewTx(key, signer, &types.LegacyTx{
				Nonce:    nonce,
				To:       &testCounterContract,
				Gas:      100_000,
				GasPrice: big.NewInt(params.InitialBaseFee),
			}))
		}
	}
	return txs
}

func importBlock(t *testing.T, chain *core.BlockChain, block *types.Block) *state.StateDB {
	t.Helper()
	if _, err := chain.InsertChain(types.Blocks{block}); err != nil {
		t.Fatalf("failed to insert parallel-built block: %v", err)
	}
	statedb, err := chain.StateAt(block.Header())
	if err != nil {
		t.Fatalf("failed to open state: %v", err)
	}
	return statedb
}

func checkCounter(t *testing.T, statedb *state.StateDB, want int) {
	t.Helper()
	counter := statedb.GetState(testCounterContract, common.Hash{})
	if counter.Big().Int64() != int64(want) {
		t.Fatalf("counter slot is %d, want %d", counter.Big().Int64(), int64(want))
	}
}

func newTestWorkerBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, n int) *testWorkerBackend {
	var gspec = &core.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			testBankAddress:     {Balance: testBankFunds},
			testUserAddress:     {Balance: new(big.Int).Set(testBankFunds)},
			testContract:        {Nonce: 1, Code: common.FromHex("0x600160005560006000a000")},
			testCounterContract: {Nonce: 1, Code: common.FromHex("0x60016000540160005500")},
		},
	}
	for _, key := range testConflictKeys {
		gspec.Alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.Account{Balance: new(big.Int).Set(testBankFunds)}
	}
	switch e := engine.(type) {
	case *clique.Clique:
		gspec.ExtraData = make([]byte, 32+common.AddressLength+crypto.SignatureLength)
		copy(gspec.ExtraData[32:32+common.AddressLength], testBankAddress.Bytes())
		e.Authorize(testBankAddress)
	case *ethash.Ethash:
	case *beacon.Beacon:
	default:
		t.Fatalf("unexpected consensus engine type: %T", engine)
	}
	chain, err := core.NewBlockChain(db, gspec, engine, &core.BlockChainConfig{ArchiveMode: true})
	if err != nil {
		t.Fatalf("core.NewBlockChain failed: %v", err)
	}
	pool := legacypool.New(testTxPoolConfig, chain)
	txpool, _ := txpool.New(testTxPoolConfig.PriceLimit, chain, []txpool.SubPool{pool})

	return &testWorkerBackend{
		db:      db,
		chain:   chain,
		txPool:  txpool,
		genesis: gspec,
	}
}

func (b *testWorkerBackend) BlockChain() *core.BlockChain { return b.chain }
func (b *testWorkerBackend) TxPool() *txpool.TxPool       { return b.txPool }

func newTestWorker(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, blocks int) (*Miner, *testWorkerBackend) {
	backend := newTestWorkerBackend(t, chainConfig, engine, db, blocks)
	backend.txPool.Add(pendingTxs, true)
	w := New(backend, testConfig, engine)
	return w, backend
}

func TestBuildPayload(t *testing.T) {
	var (
		db        = rawdb.NewMemoryDatabase()
		recipient = common.HexToAddress("0xdeadbeef")
	)
	w, b := newTestWorker(t, params.TestChainConfig, ethash.NewFaker(), db, 0)

	timestamp := uint64(time.Now().Unix())
	args := &BuildPayloadArgs{
		Parent:       b.chain.CurrentBlock().Hash(),
		Timestamp:    timestamp,
		Random:       common.Hash{},
		FeeRecipient: recipient,
	}
	payload, err := w.buildPayload(context.Background(), args, false)
	if err != nil {
		t.Fatalf("Failed to build payload %v", err)
	}
	verify := func(outer *engine.ExecutionPayloadEnvelope, txs int) {
		payload := outer.ExecutionPayload
		if payload.ParentHash != b.chain.CurrentBlock().Hash() {
			t.Fatal("Unexpected parent hash")
		}
		if payload.Random != (common.Hash{}) {
			t.Fatal("Unexpected random value")
		}
		if payload.Timestamp != timestamp {
			t.Fatal("Unexpected timestamp")
		}
		if payload.FeeRecipient != recipient {
			t.Fatal("Unexpected fee recipient")
		}
		if len(payload.Transactions) != txs {
			t.Fatal("Unexpected transaction set")
		}
	}
	empty := payload.ResolveEmpty()
	verify(empty, 0)

	full := payload.ResolveFull()
	verify(full, len(pendingTxs))

	// Ensure resolve can be called multiple times and the
	// result should be unchanged
	dataOne := payload.Resolve()
	dataTwo := payload.Resolve()
	if !reflect.DeepEqual(dataOne, dataTwo) {
		t.Fatal("Unexpected payload data")
	}
}

func TestParallelBuildPayloadMatchesSequential(t *testing.T) {
	sequential, sequentialBackend := newTestWorker(t, params.TestChainConfig, ethash.NewFaker(), rawdb.NewMemoryDatabase(), 0)
	parallel, parallelBackend := newTestWorker(t, params.TestChainConfig, ethash.NewFaker(), rawdb.NewMemoryDatabase(), 0)
	parallel.config.ParallelExecution = true
	parallel.config.ParallelWorkers = 8

	signer := types.LatestSigner(params.TestChainConfig)
	txs := counterCalls(signer, testConflictKeys, 2)
	logTx := types.MustSignNewTx(testUserKey, signer, &types.LegacyTx{
		Nonce:    0,
		To:       &testContract,
		Gas:      100_000,
		GasPrice: big.NewInt(params.InitialBaseFee),
	})
	txs = append(txs, logTx)
	sequentialBackend.txPool.Add(txs, true)
	parallelBackend.txPool.Add(txs, true)
	wantTxs := len(pendingTxs) + len(txs)

	timestamp := uint64(time.Now().Unix())
	feeRecipient := common.HexToAddress("0xdeadbeef")
	sequentialPayload, err := sequential.buildPayload(context.Background(), &BuildPayloadArgs{
		Parent:       sequentialBackend.chain.CurrentBlock().Hash(),
		Timestamp:    timestamp,
		Random:       common.Hash{},
		FeeRecipient: feeRecipient,
	}, false)
	if err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	parallelPayload, err := parallel.buildPayload(cancelled, &BuildPayloadArgs{
		Parent:       parallelBackend.chain.CurrentBlock().Hash(),
		Timestamp:    timestamp,
		Random:       common.Hash{},
		FeeRecipient: feeRecipient,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	have, want := parallelPayload.ResolveFull(), sequentialPayload.ResolveFull()

	if len(have.ExecutionPayload.Transactions) != wantTxs {
		t.Fatalf("parallel payload has %d transactions, want %d", len(have.ExecutionPayload.Transactions), wantTxs)
	}
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("parallel payload differs from sequential payload:\n have %#v\n want %#v", have, want)
	}
}

func TestParallelBuildPayloadConflicts(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	w, b := newTestWorker(t, params.MergedTestChainConfig, beacon.New(ethash.NewFaker()), db, 0)
	w.config.ParallelExecution = true
	w.config.ParallelWorkers = 8

	txs := counterCalls(types.LatestSigner(params.MergedTestChainConfig), testConflictKeys, 2)
	if errs := b.txPool.Add(txs, true); errs != nil {
		for _, err := range errs {
			if err != nil {
				t.Fatalf("failed to add transaction: %v", err)
			}
		}
	}
	wantTxs := len(pendingTxs) + len(txs)

	var (
		beaconRoot = common.Hash{0x01}
		slotNum    = uint64(1)
	)
	result := w.generateWork(context.Background(), &generateParams{
		timestamp:   1,
		forceTime:   true,
		parentHash:  b.chain.CurrentBlock().Hash(),
		coinbase:    common.HexToAddress("0xdeadbeef"),
		withdrawals: types.Withdrawals{},
		beaconRoot:  &beaconRoot,
		slotNum:     &slotNum,
	}, false)
	if result.err != nil {
		t.Fatalf("Failed to build payload %v", result.err)
	}
	attempt := w.lastParallelMetrics.Load()
	if attempt == nil {
		t.Fatal("parallel executor did not run")
	}
	if len(result.block.Transactions()) != wantTxs {
		t.Fatalf("block has %d transactions, want %d", len(result.block.Transactions()), wantTxs)
	}
	if attempt.Stale == 0 || attempt.Executions <= attempt.Planned {
		t.Fatalf("conflict path not exercised: stale %d executions %d planned %d", attempt.Stale, attempt.Executions, attempt.Planned)
	}

	// re excute the block sequentially and check if the counter
	// contracts slot has the expected value as the parallel execution.
	statedb := importBlock(t, b.chain, result.block)
	checkCounter(t, statedb, len(txs))
}

func TestPayloadId(t *testing.T) {
	t.Parallel()
	ids := make(map[string]int)
	for i, tt := range []*BuildPayloadArgs{
		{
			Parent:       common.Hash{1},
			Timestamp:    1,
			Random:       common.Hash{0x1},
			FeeRecipient: common.Address{0x1},
		},
		// Different parent
		{
			Parent:       common.Hash{2},
			Timestamp:    1,
			Random:       common.Hash{0x1},
			FeeRecipient: common.Address{0x1},
		},
		// Different timestamp
		{
			Parent:       common.Hash{2},
			Timestamp:    2,
			Random:       common.Hash{0x1},
			FeeRecipient: common.Address{0x1},
		},
		// Different Random
		{
			Parent:       common.Hash{2},
			Timestamp:    2,
			Random:       common.Hash{0x2},
			FeeRecipient: common.Address{0x1},
		},
		// Different fee-recipient
		{
			Parent:       common.Hash{2},
			Timestamp:    2,
			Random:       common.Hash{0x2},
			FeeRecipient: common.Address{0x2},
		},
		// Different withdrawals (non-empty)
		{
			Parent:       common.Hash{2},
			Timestamp:    2,
			Random:       common.Hash{0x2},
			FeeRecipient: common.Address{0x2},
			Withdrawals: []*types.Withdrawal{
				{
					Index:     0,
					Validator: 0,
					Address:   common.Address{},
					Amount:    0,
				},
			},
		},
		// Different withdrawals (non-empty)
		{
			Parent:       common.Hash{2},
			Timestamp:    2,
			Random:       common.Hash{0x2},
			FeeRecipient: common.Address{0x2},
			Withdrawals: []*types.Withdrawal{
				{
					Index:     2,
					Validator: 0,
					Address:   common.Address{},
					Amount:    0,
				},
			},
		},
	} {
		id := tt.Id().String()
		if prev, exists := ids[id]; exists {
			t.Errorf("ID collision, case %d and case %d: id %v", prev, i, id)
		}
		ids[id] = i
	}
}
