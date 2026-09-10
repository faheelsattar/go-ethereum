package miner

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
)

var dependencyBuildCounter atomic.Uint64

type storageLocation struct {
	address common.Address
	slot    common.Hash
}

type transactionAccess struct {
	hash              common.Hash
	sender            common.Address
	nonce             uint64
	index             int
	candidateRank     uint64
	gasUsed           uint64
	execution         time.Duration
	status            uint64
	effectiveTip      *big.Int
	reads             map[storageLocation]struct{}
	writes            map[storageLocation]struct{}
	attemptedWrites   map[storageLocation]struct{}
	changedWrites     map[storageLocation]struct{}
	initialValues     map[storageLocation]common.Hash
	phantomWrites     map[storageLocation]struct{}
	netZeroWrites     map[storageLocation]struct{}
	writeAttemptCount uint64
	changedWriteCount uint64
	phantomWriteCount uint64
}

// slot 0 = > 1
// . =>2
// ==> 1
type dependencyAnalyzer struct {
	buildID         uint64
	parentHash      common.Hash
	blockNumber     uint64
	started         time.Time
	baseFee         *big.Int
	state           *state.StateDB
	current         *transactionAccess
	transactions    []*transactionAccess
	candidateCount  uint64
	pendingPlainTxs int
	pendingBlobTxs  int
	summaryLogged   bool
	dotDir          string
	silent          bool
}

type dependencyPair struct {
	from int
	to   int
}

type dependencyKinds uint8

const (
	dependencyRAW dependencyKinds = 1 << iota
	dependencyWAR
	dependencyWAW
	dependencyNonce
)

type dependencySummary struct {
	transactions       int
	conflictingTxs     int
	storageConflicting int
	conflictingPairs   int
	rawPairs           int
	warPairs           int
	wawPairs           int
	noncePairs         int
	isolatedTxs        int
	criticalPath       int
	maxParallelWidth   int
	largestComponent   int
	weightedSpan       time.Duration
	totalExecution     time.Duration
	workSpan           float64
	weightedWorkSpan   float64
	phantomWrites      uint64
	writeAttempts      uint64
	changedWrites      uint64
	netZeroWrites      int
	edges              map[dependencyPair]dependencyKinds
	conflictLocations  map[storageLocation]int
	priorityFeeRevenue *big.Int
}

func newDependencyAnalyzer(buildID uint64, header *types.Header, statedb *state.StateDB, dotDir string) *dependencyAnalyzer {
	var baseFee *big.Int
	if header.BaseFee != nil {
		baseFee = new(big.Int).Set(header.BaseFee)
	}
	return &dependencyAnalyzer{
		buildID:     buildID,
		parentHash:  header.ParentHash,
		blockNumber: header.Number.Uint64(),
		started:     time.Now(),
		baseFee:     baseFee,
		state:       statedb,
		dotDir:      dotDir,
	}
}

func (a *dependencyAnalyzer) hooks() *tracing.Hooks {
	return &tracing.Hooks{OnOpcode: a.onOpcode}
}

func (a *dependencyAnalyzer) beginTransaction(tx *types.Transaction, sender common.Address, index int, candidateRank uint64) {
	tip, err := tx.EffectiveGasTip(a.baseFee)
	if err != nil {
		tip = new(big.Int)
	}
	a.current = &transactionAccess{
		hash:            tx.Hash(),
		sender:          sender,
		nonce:           tx.Nonce(),
		index:           index,
		candidateRank:   candidateRank,
		effectiveTip:    tip,
		reads:           make(map[storageLocation]struct{}),
		writes:          make(map[storageLocation]struct{}),
		attemptedWrites: make(map[storageLocation]struct{}),
		changedWrites:   make(map[storageLocation]struct{}),
		initialValues:   make(map[storageLocation]common.Hash),
		phantomWrites:   make(map[storageLocation]struct{}),
		netZeroWrites:   make(map[storageLocation]struct{}),
	}
}

func (a *dependencyAnalyzer) onOpcode(_ uint64, opcode byte, _ uint64, _ uint64, scope tracing.OpContext, _ []byte, _ int, err error) {
	if a.current == nil || err != nil {
		return
	}
	op := vm.OpCode(opcode)
	// maybe also think about CREATE, CREATE2 AND SELFDESTRUCT
	if op != vm.SLOAD && op != vm.SSTORE {
		return
	}
	stack := scope.StackData()
	if len(stack) == 0 {
		return
	}
	location := storageLocation{
		address: scope.Address(),
		slot:    common.Hash(stack[len(stack)-1].Bytes32()), // fetch slot from top of stack
	}
	current := a.state.GetState(location.address, location.slot)
	if _, ok := a.current.initialValues[location]; !ok {
		a.current.initialValues[location] = current
	}
	a.current.reads[location] = struct{}{}

	if op != vm.SSTORE || len(stack) < 2 {
		return
	}
	a.current.attemptedWrites[location] = struct{}{}
	a.current.writeAttemptCount++
	value := common.Hash(stack[len(stack)-2].Bytes32())
	if value == current {
		// sstore wrote the value already present
		a.current.phantomWrites[location] = struct{}{}
		a.current.phantomWriteCount++
	} else {
		// sstore actually changed the value here
		a.current.changedWrites[location] = struct{}{}
		a.current.changedWriteCount++
	}
}

func (a *dependencyAnalyzer) finishTransaction(receipt *types.Receipt, elapsed time.Duration) {
	tx := a.current
	a.current = nil
	if tx == nil {
		return
	}
	for location := range tx.attemptedWrites {
		// finalValue != initialValue
		if a.state.GetState(location.address, location.slot) != tx.initialValues[location] {
			// if this is the case then we can say that tx left a real state change.
			tx.writes[location] = struct{}{}
		} else if _, changed := tx.changedWrites[location]; changed {
			// state was set back to the initial value after some change
			tx.netZeroWrites[location] = struct{}{}
		}
	}
	tx.gasUsed = receipt.GasUsed
	tx.execution = elapsed
	tx.status = receipt.Status
	a.transactions = append(a.transactions, tx)

	if !a.silent {
		a.logTransaction(tx)
	}
}

func (a *dependencyAnalyzer) logTransaction(tx *transactionAccess) {
	log.Info("Block build transaction access",
		"build", a.buildID,
		"parent", a.parentHash,
		"number", a.blockNumber,
		"hash", tx.hash,
		"index", tx.index,
		"candidateRank", tx.candidateRank,
		"sender", tx.sender,
		"nonce", tx.nonce,
		"status", tx.status,
		"effectiveTip", tx.effectiveTip,
		"gasUsed", tx.gasUsed,
		"execution", tx.execution,
		"storageReads", formatStorageLocations(tx.reads),
		"storageReadCount", len(tx.reads),
		"storageWrites", formatStorageLocations(tx.writes),
		"storageWriteCount", len(tx.writes),
		"phantomWriteLocations", formatStorageLocations(tx.phantomWrites),
		"phantomWriteLocationCount", len(tx.phantomWrites),
		"netZeroWriteLocations", formatStorageLocations(tx.netZeroWrites),
		"netZeroWriteCount", len(tx.netZeroWrites),
		"writeAttempts", tx.writeAttemptCount,
		"changedWriteAttempts", tx.changedWriteCount,
		"phantomWrites", tx.phantomWriteCount,
	)
}

func (a *dependencyAnalyzer) commitParallelTransaction(tx *transactionAccess, index int, candidateRank uint64) {
	if tx == nil {
		return
	}
	tx.index = index
	tx.candidateRank = candidateRank
	a.transactions = append(a.transactions, tx)
	a.logTransaction(tx)
}

func (a *dependencyAnalyzer) abortTransaction() {
	a.current = nil
}

func (a *dependencyAnalyzer) nextCandidate() uint64 {
	a.candidateCount++
	return a.candidateCount
}

func (a *dependencyAnalyzer) setPendingCounts(plain, blob int) {
	a.pendingPlainTxs = plain
	a.pendingBlobTxs = blob
}

func (a *dependencyAnalyzer) logCandidate(rank uint64, hash common.Hash, decision, reason string, err error) {
	context := []any{
		"build", a.buildID,
		"parent", a.parentHash,
		"number", a.blockNumber,
		"candidateRank", rank,
		"hash", hash,
		"decision", decision,
		"reason", reason,
	}
	if err != nil {
		context = append(context, "err", err)
	}
	log.Info("Block build candidate decision", context...)
}

func (a *dependencyAnalyzer) logSummary() {
	if a.summaryLogged {
		return
	}
	a.summaryLogged = true
	summary := analyzeDependencies(a.transactions)
	log.Info("Block build dependency summary",
		"build", a.buildID,
		"parent", a.parentHash,
		"number", a.blockNumber,
		"elapsed", time.Since(a.started),
		"pendingPlain", a.pendingPlainTxs,
		"pendingBlob", a.pendingBlobTxs,
		"candidates", a.candidateCount,
		"transactions", summary.transactions,
		"conflictingTxs", summary.conflictingTxs,
		"storageConflictingTxs", summary.storageConflicting,
		"conflictingPairs", summary.conflictingPairs,
		"conflictLocations", len(summary.conflictLocations),
		"rawPairs", summary.rawPairs,
		"warPairs", summary.warPairs,
		"wawPairs", summary.wawPairs,
		"noncePairs", summary.noncePairs,
		"isolatedTxs", summary.isolatedTxs,
		"isolatedTxPercent", fmt.Sprintf("%.2f", percentage(summary.isolatedTxs, summary.transactions)),
		"criticalPath", summary.criticalPath,
		"maxParallelWidth", summary.maxParallelWidth,
		"largestComponent", summary.largestComponent,
		"giantComponentPercent", fmt.Sprintf("%.2f", percentage(summary.largestComponent, summary.transactions)),
		"averageDegree", fmt.Sprintf("%.2f", averageDegree(summary.conflictingPairs, summary.transactions)),
		"workSpan", fmt.Sprintf("%.2f", summary.workSpan),
		"totalExecution", summary.totalExecution,
		"weightedCriticalPath", summary.weightedSpan,
		"weightedWorkSpan", fmt.Sprintf("%.2f", summary.weightedWorkSpan),
		"phantomWrites", summary.phantomWrites,
		"writeAttempts", summary.writeAttempts,
		"changedWriteAttempts", summary.changedWrites,
		"phantomWritePercent", fmt.Sprintf("%.2f", percentageUint64(summary.phantomWrites, summary.writeAttempts)),
		"netZeroWrites", summary.netZeroWrites,
		"priorityFeeRevenue", summary.priorityFeeRevenue,
		"hotStorageLocations", formatHotStorageLocations(summary.conflictLocations, 10),
	)
}

// t0 -> t1
// t1 -> t2
// t3

// t0 -> t1 -> t2 // dependent
// t3 // alone

// takes the collected sets for all included txs, in block, and
// constructs a directed dependency graph.
func analyzeDependencies(transactions []*transactionAccess) dependencySummary {
	summary := dependencySummary{
		transactions:       len(transactions),
		edges:              make(map[dependencyPair]dependencyKinds),
		conflictLocations:  make(map[storageLocation]int),
		priorityFeeRevenue: new(big.Int),
	}
	// for each storage location, this remembers the most recent
	// tx that wrote it
	lastWriter := make(map[storageLocation]int)
	// for each location, this remembers txs that read the
	// location since its last write
	readers := make(map[storageLocation]map[int]struct{})
	// remembers the most recent transaction from each sender
	// tx from same sender must remain nonce ordered
	lastSender := make(map[common.Address]int)

	addEdge := func(from, to int, kind dependencyKinds) {
		if from == to {
			return
		}
		summary.edges[dependencyPair{from: from, to: to}] |= kind
	}
	for index, tx := range transactions {
		// process reads
		for location := range tx.reads {
			if writer, ok := lastWriter[location]; ok {
				// e.g: t0 → t1, RAW (READ AFTER WRITE)
				// t1 is reading a state that was already written by t0
				addEdge(writer, index, dependencyRAW)
				summary.conflictLocations[location]++
			}
			if readers[location] == nil {
				readers[location] = make(map[int]struct{})
			}
			// register the current tx as a reader
			readers[location][index] = struct{}{}
		}

		// process writes
		for location := range tx.writes {
			// check for detecing WAW (WRITE AFTER WRITE)
			if writer, ok := lastWriter[location]; ok {
				// to writes x to location
				// t2 write x to location

				// to → t2, WAW
				addEdge(writer, index, dependencyWAW)
				summary.conflictLocations[location]++
			}
			// check for detecing WAR (WRITE AFTER READ)
			for reader := range readers[location] {
				// t1 reads x from location
				// t2 writex x to location
				// t1 → t2, WAR
				addEdge(reader, index, dependencyWAR)
				if reader != index {
					summary.conflictLocations[location]++
				}
			}
			// reset the reader and update the writer
			readers[location] = make(map[int]struct{})
			lastWriter[location] = index
		}
		// add sender nonce dependencies
		if previous, ok := lastSender[tx.sender]; ok {
			addEdge(previous, index, dependencyNonce)
		}
		lastSender[tx.sender] = index
		summary.totalExecution += tx.execution
		summary.phantomWrites += tx.phantomWriteCount
		summary.writeAttempts += tx.writeAttemptCount
		summary.changedWrites += tx.changedWriteCount
		summary.netZeroWrites += len(tx.netZeroWrites)
		if tx.effectiveTip != nil {
			fee := new(big.Int).Mul(new(big.Int).SetUint64(tx.gasUsed), tx.effectiveTip)
			summary.priorityFeeRevenue.Add(summary.priorityFeeRevenue, fee)
		}
	}
	summary.populateGraphMetrics(transactions)
	return summary
}

func (s *dependencySummary) populateGraphMetrics(transactions []*transactionAccess) {
	if len(transactions) == 0 {
		return
	}
	degree, storageDegree, outgoing := s.indexEdges(len(transactions))
	depth, span := dependencyPaths(transactions, outgoing)

	s.conflictingPairs = len(s.edges)
	s.largestComponent = largestConnectedComponent(len(transactions), s.edges)
	for i := range transactions {
		if degree[i] == 0 {
			s.isolatedTxs++
		} else {
			s.conflictingTxs++
		}
		if storageDegree[i] != 0 {
			s.storageConflicting++
		}
		s.criticalPath = max(s.criticalPath, depth[i])
		s.weightedSpan = max(s.weightedSpan, span[i])
	}
	s.maxParallelWidth = widestLayer(depth)
	s.workSpan = float64(len(transactions)) / float64(s.criticalPath)
	if s.weightedSpan > 0 {
		s.weightedWorkSpan = float64(s.totalExecution) / float64(s.weightedSpan)
	}
}

// indexEdges counts edge types and builds the adjacency list used by the path calculation.
func (s *dependencySummary) indexEdges(transactionCount int) ([]int, []int, [][]int) {
	degree, storageDegree, outgoing := graphIndex(transactionCount, s.edges)
	for _, kinds := range s.edges {
		if kinds&dependencyRAW != 0 {
			s.rawPairs++
		}
		if kinds&dependencyWAR != 0 {
			s.warPairs++
		}
		if kinds&dependencyWAW != 0 {
			s.wawPairs++
		}
		if kinds&dependencyNonce != 0 {
			s.noncePairs++
		}
	}
	return degree, storageDegree, outgoing
}

// dependencyPaths calculates the transaction-count and execution-time path ending at each node.
func dependencyPaths(transactions []*transactionAccess, outgoing [][]int) ([]int, []time.Duration) {
	depth := make([]int, len(transactions))
	span := make([]time.Duration, len(transactions))
	for i, tx := range transactions {
		depth[i] = 1
		span[i] = tx.execution
	}
	for from, targets := range outgoing {
		for _, to := range targets {
			depth[to] = max(depth[to], depth[from]+1)
			span[to] = max(span[to], span[from]+transactions[to].execution)
		}
	}
	return depth, span
}

func widestLayer(depth []int) int {
	widths := make(map[int]int)
	widest := 0
	for _, layer := range depth {
		widths[layer]++
		widest = max(widest, widths[layer])
	}
	return widest
}

type disjointSet struct {
	parent []int
	size   []int
}

func newDisjointSet(count int) *disjointSet {
	set := &disjointSet{parent: make([]int, count), size: make([]int, count)}
	for i := range count {
		set.parent[i] = i
		set.size[i] = 1
	}
	return set
}

func (s *disjointSet) find(node int) int {
	for node != s.parent[node] {
		s.parent[node] = s.parent[s.parent[node]]
		node = s.parent[node]
	}
	return node
}

func (s *disjointSet) union(a, b int) {
	a, b = s.find(a), s.find(b)
	if a == b {
		return
	}
	if s.size[a] < s.size[b] {
		a, b = b, a
	}
	s.parent[b] = a
	s.size[a] += s.size[b]
}

func largestConnectedComponent(transactionCount int, edges map[dependencyPair]dependencyKinds) int {
	set := newDisjointSet(transactionCount)
	for pair := range edges {
		set.union(pair.from, pair.to)
	}
	largest := 0
	for i := range transactionCount {
		largest = max(largest, set.size[set.find(i)])
	}
	return largest
}

func (a *dependencyAnalyzer) writeDOT(summary dependencySummary) (string, error) {
	if err := os.MkdirAll(a.dotDir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("dependency-%d-%d-%d.dot", a.blockNumber, a.started.UnixNano(), a.buildID)
	path := filepath.Join(a.dotDir, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := file.WriteString(renderDependencyDOT(a, summary)); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func renderDependencyDOT(analyzer *dependencyAnalyzer, summary dependencySummary) string {
	var dot strings.Builder
	fmt.Fprintf(&dot, "digraph dependencies {\n  rankdir=LR;\n  graph [label=%q, labelloc=t];\n",
		fmt.Sprintf("block %d, build %d", analyzer.blockNumber, analyzer.buildID))
	dot.WriteString("  node [shape=box, style=filled, fontname=Helvetica];\n")
	dot.WriteString("  edge [fontname=Helvetica];\n")
	dot.WriteString("  subgraph cluster_legend {\n")
	dot.WriteString("    label=\"Legend\"; fontsize=10;\n")
	dot.WriteString("    independent [label=\"independent\", fillcolor=\"#b7e4c7\"];\n")
	dot.WriteString("    storage [label=\"storage conflict\", fillcolor=\"#f8c291\"];\n")
	dot.WriteString("    nonce [label=\"nonce only\", fillcolor=\"#fff2a8\"];\n")
	dot.WriteString("  }\n")

	degree, storageDegree, outgoing := graphIndex(len(analyzer.transactions), summary.edges)
	depth, _ := dependencyPaths(analyzer.transactions, outgoing)
	for i, tx := range analyzer.transactions {
		color := "#fff2a8" // Nonce-only dependency.
		switch {
		case degree[i] == 0:
			color = "#b7e4c7" // Independent transaction.
		case storageDegree[i] != 0:
			color = "#f8c291" // Storage conflict.
		}
		label := fmt.Sprintf("tx %d\n%s\ngas %d | %s\nreads %d | writes %d",
			tx.index, shortHash(tx.hash), tx.gasUsed, tx.execution, len(tx.reads), len(tx.writes))
		fmt.Fprintf(&dot, "  tx%d [label=%q, fillcolor=%q, tooltip=%q];\n", i, label, color, tx.hash.Hex())
	}
	for layer := 1; layer <= summary.criticalPath; layer++ {
		fmt.Fprintf(&dot, "  { rank=same; // dependency layer %d\n", layer)
		for transaction, transactionLayer := range depth {
			if transactionLayer == layer {
				fmt.Fprintf(&dot, "    tx%d;\n", transaction)
			}
		}
		dot.WriteString("  }\n")
	}
	for _, pair := range sortedDependencyPairs(summary.edges) {
		kinds := summary.edges[pair]
		fmt.Fprintf(&dot, "  tx%d -> tx%d [label=%q, color=%q];\n",
			pair.from, pair.to, dependencyLabel(kinds), dependencyColor(kinds))
	}
	dot.WriteString("}\n")
	return dot.String()
}

func graphIndex(transactionCount int, edges map[dependencyPair]dependencyKinds) ([]int, []int, [][]int) {
	degree := make([]int, transactionCount)
	storageDegree := make([]int, transactionCount)
	outgoing := make([][]int, transactionCount)
	for pair, kinds := range edges {
		degree[pair.from]++
		degree[pair.to]++
		outgoing[pair.from] = append(outgoing[pair.from], pair.to)
		if kinds&(dependencyRAW|dependencyWAR|dependencyWAW) != 0 {
			storageDegree[pair.from]++
			storageDegree[pair.to]++
		}
	}
	return degree, storageDegree, outgoing
}

func sortedDependencyPairs(edges map[dependencyPair]dependencyKinds) []dependencyPair {
	pairs := make([]dependencyPair, 0, len(edges))
	for pair := range edges {
		pairs = append(pairs, pair)
	}
	slices.SortFunc(pairs, func(a, b dependencyPair) int {
		if a.from != b.from {
			return a.from - b.from
		}
		return a.to - b.to
	})
	return pairs
}

func dependencyLabel(kinds dependencyKinds) string {
	labels := make([]string, 0, 4)
	if kinds&dependencyRAW != 0 {
		labels = append(labels, "RAW")
	}
	if kinds&dependencyWAR != 0 {
		labels = append(labels, "WAR")
	}
	if kinds&dependencyWAW != 0 {
		labels = append(labels, "WAW")
	}
	if kinds&dependencyNonce != 0 {
		labels = append(labels, "NONCE")
	}
	return strings.Join(labels, ",")
}

func dependencyColor(kinds dependencyKinds) string {
	if kinds&(kinds-1) != 0 {
		return "#343a40" // Multiple dependency kinds.
	}
	switch kinds {
	case dependencyRAW:
		return "#d62828"
	case dependencyWAR:
		return "#f77f00"
	case dependencyWAW:
		return "#6a4c93"
	default:
		return "#6c757d"
	}
}

func shortHash(hash common.Hash) string {
	return hash.Hex()[:12]
}

func formatStorageLocations(locations map[storageLocation]struct{}) []string {
	result := make([]string, 0, len(locations))
	for location := range locations {
		result = append(result, location.address.Hex()+":"+location.slot.Hex())
	}
	slices.Sort(result)
	return result
}

func formatHotStorageLocations(locations map[storageLocation]int, limit int) []string {
	type hotLocation struct {
		location  storageLocation
		conflicts int
	}
	hot := make([]hotLocation, 0, len(locations))
	for location, conflicts := range locations {
		hot = append(hot, hotLocation{location: location, conflicts: conflicts})
	}
	slices.SortFunc(hot, func(a, b hotLocation) int {
		if a.conflicts != b.conflicts {
			return b.conflicts - a.conflicts
		}
		left := a.location.address.Hex() + a.location.slot.Hex()
		right := b.location.address.Hex() + b.location.slot.Hex()
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	})
	if len(hot) > limit {
		hot = hot[:limit]
	}
	result := make([]string, len(hot))
	for i, entry := range hot {
		result[i] = fmt.Sprintf("%s:%s:%d", entry.location.address.Hex(), entry.location.slot.Hex(), entry.conflicts)
	}
	return result
}

func percentage(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}

func averageDegree(edges, vertices int) float64 {
	if vertices == 0 {
		return 0
	}
	return 2 * float64(edges) / float64(vertices)
}

func percentageUint64(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}
