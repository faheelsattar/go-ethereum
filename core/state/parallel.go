// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package state

import (
	"bytes"
	"maps"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/holiman/uint256"
)

// ParallelAccountFields identifies independently versioned account fields.
type ParallelAccountFields uint8

const (
	ParallelAccountExistence ParallelAccountFields = 1 << iota
	ParallelAccountBalance
	ParallelAccountNonce
	ParallelAccountCode
)

const parallelAccountAll = ParallelAccountExistence | ParallelAccountBalance | ParallelAccountNonce | ParallelAccountCode

// ParallelStorageLocation identifies one persistent storage slot.
type ParallelStorageLocation struct {
	Address common.Address
	Slot    common.Hash
}

type ParallelConflictKind uint8

const (
	ParallelAccountConflict ParallelConflictKind = iota
	ParallelStorageConflict
)

// ParallelStateConflict describes the first state dependency found between a
// speculative result and an earlier committed result.
type ParallelStateConflict struct {
	Kind          ParallelConflictKind
	Address       common.Address
	Slot          common.Hash
	AccountFields ParallelAccountFields
}

// ParallelStateAccesses contains the consensus-state footprint of one
// transaction execution.
type ParallelStateAccesses struct {
	AccountReads  map[common.Address]ParallelAccountFields
	AccountWrites map[common.Address]ParallelAccountFields
	StorageReads  map[ParallelStorageLocation]struct{}
	StorageWrites map[ParallelStorageLocation]struct{}
}

// ParallelAccountChange contains final values changed by one transaction.
// BalanceDelta is used only for transaction-fee credits, which are additive.
type ParallelAccountChange struct {
	Created         bool
	CreatedContract bool
	SelfDestructed  bool
	Touched         bool
	Balance         *uint256.Int
	BalanceDelta    *uint256.Int
	Nonce           *uint64
	Code            []byte
	CodeChanged     bool
	Storage         map[common.Hash]common.Hash
}

// ParallelStateResult is a transaction-local state delta. It can be applied
// only if Conflicts reports no stale reads against earlier results.
type ParallelStateResult struct {
	Accesses  ParallelStateAccesses
	Accounts  map[common.Address]*ParallelAccountChange
	Preimages map[common.Hash][]byte
	Logs      []*types.Log
	Amsterdam bool
}

type parallelStateRecorder struct {
	accesses       ParallelStateAccesses
	feeCredits     map[common.Address]*uint256.Int
	nonFeeBalances map[common.Address]struct{}
	preimages      map[common.Hash]struct{}
	result         *ParallelStateResult
}

func newParallelStateRecorder(preimages map[common.Hash][]byte) *parallelStateRecorder {
	known := make(map[common.Hash]struct{}, len(preimages))
	for hash := range preimages {
		known[hash] = struct{}{}
	}
	return &parallelStateRecorder{
		accesses: ParallelStateAccesses{
			AccountReads:  make(map[common.Address]ParallelAccountFields),
			AccountWrites: make(map[common.Address]ParallelAccountFields),
			StorageReads:  make(map[ParallelStorageLocation]struct{}),
			StorageWrites: make(map[ParallelStorageLocation]struct{}),
		},
		feeCredits:     make(map[common.Address]*uint256.Int),
		nonFeeBalances: make(map[common.Address]struct{}),
		preimages:      known,
	}
}

// parallelBaseReader is a reader over a statedbs uncommitted block state,
// falling back to its underlying reader. It reads the bases maps without
// locks, so the base must stay unmodified while the reader is in use.
type parallelBaseReader struct {
	base *StateDB
}

func (r *parallelBaseReader) Account(addr common.Address) (*types.StateAccount, error) {
	if obj := r.base.stateObjects[addr]; obj != nil {
		return obj.data.Copy(), nil
	}
	// Accounts destructed in this block (and not resurrected) do not exist,
	// regardless of what the disk state says.
	if _, ok := r.base.stateObjectsDestruct[addr]; ok {
		return nil, nil
	}
	return r.base.reader.Account(addr)
}

func (r *parallelBaseReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	if obj := r.base.stateObjects[addr]; obj != nil {
		// Mirror stateObject.GetState: dirty (should be empty between
		// transactions), then pending block writes, then the clean cache.
		if value, ok := obj.dirtyStorage[slot]; ok {
			return value, nil
		}
		if value, ok := obj.pendingStorage[slot]; ok {
			return value, nil
		}
		if value, ok := obj.originStorage[slot]; ok {
			return value, nil
		}
	}
	// If the account was destructed in this block, its old storage must not
	// be consulted: a resurrected account's live slots are all in
	// pendingStorage, everything else reads as empty.
	if _, ok := r.base.stateObjectsDestruct[addr]; ok {
		return common.Hash{}, nil
	}
	return r.base.reader.Storage(addr, slot)
}

func (r *parallelBaseReader) Code(addr common.Address, codeHash common.Hash) []byte {
	if obj := r.base.stateObjects[addr]; obj != nil && obj.code != nil && common.BytesToHash(obj.CodeHash()) == codeHash {
		return obj.code
	}
	return r.base.reader.Code(addr, codeHash)
}

func (r *parallelBaseReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	if obj := r.base.stateObjects[addr]; obj != nil && obj.code != nil && common.BytesToHash(obj.CodeHash()) == codeHash {
		return len(obj.code)
	}
	return r.base.reader.CodeSize(addr, codeHash)
}

func (r *parallelBaseReader) Has(addr common.Address, codeHash common.Hash) bool {
	if obj := r.base.stateObjects[addr]; obj != nil && obj.code != nil && common.BytesToHash(obj.CodeHash()) == codeHash {
		return true
	}
	return r.base.reader.Has(addr, codeHash)
}

// Speculative returns a fresh statedb reading through s's uncommitted block
// state without copying it. s must stay unmodified while the returned state
// is in use, and the result is throwaway: never commit it or compute roots.
func (s *StateDB) Speculative() (*StateDB, error) {
	return NewWithReader(s.originalRoot, s.db, &parallelBaseReader{base: s})
}

// StartParallelRecording enables transaction-local state access recording.
// It must be called after SetTxContext and before executing the transaction.
func (s *StateDB) StartParallelRecording() {
	s.parallelRecorder = newParallelStateRecorder(s.preimages)
}

// FinishParallelRecording returns the execution footprint captured by
// Finalise and disables recording.
func (s *StateDB) FinishParallelRecording() *ParallelStateResult {
	recorder := s.parallelRecorder
	s.parallelRecorder = nil
	if recorder == nil {
		return nil
	}
	if recorder.result == nil {
		recorder.result = &ParallelStateResult{
			Accesses:  recorder.accesses,
			Accounts:  make(map[common.Address]*ParallelAccountChange),
			Preimages: make(map[common.Hash][]byte),
		}
	}
	return recorder.result
}

func (s *StateDB) recordAccountRead(addr common.Address, fields ParallelAccountFields) {
	if s.parallelRecorder != nil {
		s.parallelRecorder.accesses.AccountReads[addr] |= fields | ParallelAccountExistence
	}
}

func (s *StateDB) recordAccountWrite(addr common.Address, fields ParallelAccountFields) {
	if s.parallelRecorder != nil {
		s.parallelRecorder.accesses.AccountWrites[addr] |= fields
	}
}

func (s *StateDB) recordStorageRead(addr common.Address, slot common.Hash) {
	if s.parallelRecorder == nil {
		return
	}
	s.recordAccountRead(addr, ParallelAccountExistence)
	s.parallelRecorder.accesses.StorageReads[ParallelStorageLocation{Address: addr, Slot: slot}] = struct{}{}
}

func (s *StateDB) recordStorageWrite(addr common.Address, slot common.Hash) {
	if s.parallelRecorder == nil {
		return
	}
	s.recordStorageRead(addr, slot)
	s.parallelRecorder.accesses.StorageWrites[ParallelStorageLocation{Address: addr, Slot: slot}] = struct{}{}
}

func (s *StateDB) recordBalanceWrite(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	if s.parallelRecorder == nil {
		return
	}

	if amount.IsZero() {
		return
	}
	s.recordAccountWrite(addr, ParallelAccountBalance)
	if reason == tracing.BalanceIncreaseRewardTransactionFee {
		delta := s.parallelRecorder.feeCredits[addr]
		if delta == nil {
			delta = new(uint256.Int)
			s.parallelRecorder.feeCredits[addr] = delta
		}
		delta.Add(delta, amount)
		return
	}
	s.parallelRecorder.nonFeeBalances[addr] = struct{}{}
	s.recordAccountRead(addr, ParallelAccountBalance)
}

func (r *parallelStateRecorder) capture(s *StateDB) {
	result := &ParallelStateResult{
		Accesses:  r.accesses,
		Accounts:  make(map[common.Address]*ParallelAccountChange),
		Preimages: make(map[common.Hash][]byte),
		Amsterdam: s.stateAccessList != nil,
	}
	for addr, mutation := range s.journal.mutations {
		change := &ParallelAccountChange{
			Touched:        mutation.counts[journalMutationKindTouch] > 0,
			Created:        mutation.counts[journalMutationKindCreate] > 0,
			SelfDestructed: mutation.counts[journalMutationKindSelfDestruct] > 0,
		}
		obj := s.stateObjects[addr]

		if change.Created && !change.SelfDestructed && obj != nil && obj.empty() && !mutation.balanceSet && !mutation.nonceSet && !mutation.codeSet {
			r.accesses.AccountReads[addr] |= ParallelAccountExistence
			continue
		}
		if obj != nil && obj.empty() {
			r.accesses.AccountWrites[addr] |= ParallelAccountExistence
		}
		if change.Touched {
			r.accesses.AccountReads[addr] |= ParallelAccountExistence
			r.accesses.AccountWrites[addr] |= ParallelAccountExistence
		}
		if change.Created || change.SelfDestructed {
			r.accesses.AccountReads[addr] |= parallelAccountAll
			r.accesses.AccountWrites[addr] |= parallelAccountAll
		}
		if mutation.balanceSet && obj != nil {
			r.accesses.AccountWrites[addr] |= ParallelAccountBalance
			if delta := r.feeCredits[addr]; delta != nil {
				_, nonFee := r.nonFeeBalances[addr]
				read := r.accesses.AccountReads[addr]&ParallelAccountBalance != 0
				if !nonFee && !read {
					change.BalanceDelta = delta.Clone()
				} else {
					change.Balance = obj.Balance().Clone()
				}
			} else {
				r.accesses.AccountReads[addr] |= ParallelAccountExistence | ParallelAccountBalance
				change.Balance = obj.Balance().Clone()
			}
		}
		if mutation.nonceSet && obj != nil {
			r.accesses.AccountReads[addr] |= ParallelAccountExistence | ParallelAccountNonce
			r.accesses.AccountWrites[addr] |= ParallelAccountNonce
			nonce := obj.Nonce()
			change.Nonce = &nonce
		}
		if mutation.codeSet {
			r.accesses.AccountReads[addr] |= ParallelAccountExistence | ParallelAccountCode
			r.accesses.AccountWrites[addr] |= ParallelAccountCode
			change.CodeChanged = true
			if obj != nil {
				change.Code = bytes.Clone(obj.Code())
			}
		}
		result.Accounts[addr] = change
	}
	for _, entry := range s.journal.entries {
		switch entry := entry.(type) {
		case createContractChange:
			r.accesses.AccountReads[entry.account] |= ParallelAccountExistence
			r.accesses.AccountWrites[entry.account] |= parallelAccountAll
			result.account(entry.account).CreatedContract = true
		case storageChange:
			location := ParallelStorageLocation{Address: entry.account, Slot: entry.key}
			r.accesses.AccountReads[entry.account] |= ParallelAccountExistence
			r.accesses.StorageReads[location] = struct{}{}
			r.accesses.StorageWrites[location] = struct{}{}
			change := result.account(entry.account)
			if change.Storage == nil {
				change.Storage = make(map[common.Hash]common.Hash)
			}
			if obj := s.stateObjects[entry.account]; obj != nil {
				value, _ := obj.getState(entry.key)
				change.Storage[entry.key] = value
			}
		}
	}
	for hash, preimage := range s.preimages {
		if _, ok := r.preimages[hash]; !ok {
			result.Preimages[hash] = slices.Clone(preimage)
		}
	}
	for _, entry := range s.logs[s.thash] {
		log := *entry
		log.Data = bytes.Clone(entry.Data)
		log.Topics = slices.Clone(entry.Topics)
		result.Logs = append(result.Logs, &log)
	}
	r.result = result
}

func (r *ParallelStateResult) account(addr common.Address) *ParallelAccountChange {
	change := r.Accounts[addr]
	if change == nil {
		change = &ParallelAccountChange{}
		r.Accounts[addr] = change
	}
	return change
}

// Conflict returns the first state dependency found where this result read
// state modified by an earlier result that was not part of its speculative
// input state.
func (r *ParallelStateResult) Conflict(previous *ParallelStateResult) *ParallelStateConflict {
	if r == nil || previous == nil {
		return nil
	}
	for addr, reads := range r.Accesses.AccountReads {
		writes := previous.Accesses.AccountWrites[addr]
		if writes&ParallelAccountExistence != 0 {
			return &ParallelStateConflict{Kind: ParallelAccountConflict, Address: addr, AccountFields: ParallelAccountExistence}
		}
		if fields := reads & writes; fields != 0 {
			return &ParallelStateConflict{Kind: ParallelAccountConflict, Address: addr, AccountFields: fields}
		}
	}
	for location := range r.Accesses.StorageReads {
		if _, ok := previous.Accesses.StorageWrites[location]; ok {
			return &ParallelStateConflict{Kind: ParallelStorageConflict, Address: location.Address, Slot: location.Slot}
		}
		if previous.Accesses.AccountWrites[location.Address]&ParallelAccountExistence != 0 {
			return &ParallelStateConflict{Kind: ParallelAccountConflict, Address: location.Address, AccountFields: ParallelAccountExistence}
		}
	}
	for addr, writes := range r.Accesses.AccountWrites {
		if writes != 0 && previous.Accesses.AccountWrites[addr]&ParallelAccountExistence != 0 {
			return &ParallelStateConflict{Kind: ParallelAccountConflict, Address: addr, AccountFields: ParallelAccountExistence}
		}
	}
	return nil
}

// Conflicts reports whether this result has a stale state dependency on an
// earlier result.
func (r *ParallelStateResult) Conflicts(previous *ParallelStateResult) bool {
	return r.Conflict(previous) != nil
}

// ApplyParallelResult merges a non-conflicting transaction result into s and
// finalises it at the transaction boundary.
func (s *StateDB) ApplyParallelResult(result *ParallelStateResult) {
	if result == nil {
		return
	}
	if result.Amsterdam {
		s.stateAccessList = bal.NewConstructionBlockAccessList()
	}
	addresses := slices.SortedFunc(maps.Keys(result.Accounts), func(a, b common.Address) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, addr := range addresses {
		change := result.Accounts[addr]
		if change.Created {
			s.CreateAccount(addr)
		}
		if change.CreatedContract {
			s.CreateContract(addr)
		}
		if change.BalanceDelta != nil {
			s.AddBalance(addr, change.BalanceDelta, tracing.BalanceIncreaseRewardTransactionFee)
		} else if change.Balance != nil {
			s.SetBalance(addr, change.Balance, tracing.BalanceChangeUnspecified)
		}
		if change.Nonce != nil {
			s.SetNonce(addr, *change.Nonce, tracing.NonceChangeUnspecified)
		}
		if change.CodeChanged {
			s.SetCode(addr, change.Code, tracing.CodeChangeUnspecified)
		}
		for slot, value := range change.Storage {
			s.SetState(addr, slot, value)
		}
		if change.Touched {
			s.Touch(addr)
		}
		if change.SelfDestructed {
			s.SelfDestruct(addr)
		}
	}
	for hash, preimage := range result.Preimages {
		s.AddPreimage(hash, preimage)
	}
	for _, entry := range result.Logs {
		log := *entry
		log.Data = bytes.Clone(entry.Data)
		log.Topics = slices.Clone(entry.Topics)
		s.AddLog(&log)
	}
	s.Finalise(true)
}
