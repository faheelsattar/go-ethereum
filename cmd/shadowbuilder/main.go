package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"
)

func main() {
	var (
		engineURL = flag.String("engine", "http://127.0.0.1:8551", "geth authenticated Engine API URL")
		jwtPath   = flag.String("jwt", "", "path to geth's JWT secret")
		fee       = flag.String("fee-recipient", common.Address{}.Hex(), "suggested payload fee recipient")
		randao    = flag.String("prev-randao", common.Hash{}.Hex(), "prevRandao for the payload")
		beacon    = flag.String("parent-beacon-root", common.Hash{}.Hex(), "parent beacon block root")
		timestamp = flag.Uint64("timestamp", 0, "payload timestamp (default: parent timestamp + 12)")
		buildTime = flag.Duration("build-time", 3*time.Second, "time to let geth build before retrieving the payload")
		version   = flag.Int("getpayload-version", 5, "engine_getPayload version (3, 4, or 5)")
		validate  = flag.Bool("validate", true, "feed the built payload back through engine_newPayload to validate it")
	)
	flag.Parse()

	if *jwtPath == "" {
		log.Fatal("-jwt is required")
	}
	if _, err := os.Stat(*jwtPath); err != nil {
		log.Fatalf("cannot access JWT secret: %v", err)
	}
	if !common.IsHexAddress(*fee) {
		log.Fatalf("invalid fee recipient %q", *fee)
	}
	if *version < 3 || *version > 5 {
		log.Fatalf("unsupported getPayload version %d", *version)
	}

	prevRandao := parseHash("prev-randao", *randao)
	parentBeaconRoot := parseHash("parent-beacon-root", *beacon)
	client := dialEngine(context.Background(), *engineURL, *jwtPath)
	defer client.Close()

	ctx := context.Background()
	head := getHeader(ctx, client, "latest")
	safe := getHeader(ctx, client, "safe")
	finalized := getHeader(ctx, client, "finalized")

	payloadTimestamp := *timestamp
	if payloadTimestamp == 0 {
		payloadTimestamp = head.Time + 12
	}
	if payloadTimestamp <= head.Time {
		log.Fatalf("payload timestamp %d must be greater than parent timestamp %d", payloadTimestamp, head.Time)
	}
	state := engine.ForkchoiceStateV1{
		HeadBlockHash:      head.Hash(),
		SafeBlockHash:      safe.Hash(),
		FinalizedBlockHash: finalized.Hash(),
	}
	attributes := engine.PayloadAttributes{
		Timestamp:             payloadTimestamp,
		Random:                prevRandao,
		SuggestedFeeRecipient: common.HexToAddress(*fee),
		Withdrawals:           make([]*types.Withdrawal, 0),
		BeaconRoot:            &parentBeaconRoot,
	}

	var response engine.ForkChoiceResponse
	if err := client.CallContext(ctx, &response, "engine_forkchoiceUpdatedV3", state, &attributes); err != nil {
		log.Fatalf("engine_forkchoiceUpdatedV3 failed: %v", err)
	}
	if response.PayloadStatus.Status != engine.VALID {
		log.Fatalf("forkchoice update returned status %s: %v", response.PayloadStatus.Status, response.PayloadStatus.ValidationError)
	}
	if response.PayloadID == nil {
		log.Fatal("forkchoice update returned no payload ID")
	}
	fmt.Printf("building payload %s on block %d (%s)\n", response.PayloadID, head.Number.Uint64(), head.Hash())

	time.Sleep(*buildTime)
	method := fmt.Sprintf("engine_getPayloadV%d", *version)
	var payload engine.ExecutionPayloadEnvelope
	if err := client.CallContext(ctx, &payload, method, *response.PayloadID); err != nil {
		log.Fatalf("%s failed: %v", method, err)
	}
	if payload.ExecutionPayload == nil {
		log.Fatal("geth returned an empty execution payload")
	}
	result := payload.ExecutionPayload
	fmt.Printf("built shadow block number=%d hash=%s transactions=%d gasUsed=%d value=%s\n",
		result.Number, result.BlockHash, len(result.Transactions), result.GasUsed, payload.BlockValue)

	// Feed the block back through engine_newPayload: geth re-executes it
	// sequentially and verifies the state root, so an incorrect block from
	// the (experimental) parallel builder is caught here instead of being
	// silently discarded unchecked.
	if *validate {
		var (
			hashes = collectVersionedHashes(result)
			status engine.PayloadStatusV1
			err    error
		)
		switch *version {
		case 3:
			err = client.CallContext(ctx, &status, "engine_newPayloadV3", result, hashes, parentBeaconRoot)
		case 4:
			err = client.CallContext(ctx, &status, "engine_newPayloadV4", result, hashes, parentBeaconRoot, requestsOrEmpty(payload.Requests))
		default:
			err = client.CallContext(ctx, &status, "engine_newPayloadV5", result, hashes, parentBeaconRoot, requestsOrEmpty(payload.Requests))
		}
		if err != nil {
			log.Fatalf("engine_newPayload validation call failed: %v", err)
		}
		if status.Status != engine.VALID {
			log.Fatalf("built payload is INVALID: status %s: %v", status.Status, status.ValidationError)
		}
		fmt.Println("payload validated by engine_newPayload (re-executed, state root verified)")
	}
	fmt.Println("payload discarded; it was not published")
}

// collectVersionedHashes gathers the blob versioned hashes of every blob
// transaction in the payload, in block order, as engine_newPayload expects.
func collectVersionedHashes(payload *engine.ExecutableData) []common.Hash {
	hashes := make([]common.Hash, 0)
	for _, data := range payload.Transactions {
		var tx types.Transaction
		if err := tx.UnmarshalBinary(data); err != nil {
			log.Fatalf("failed to decode payload transaction: %v", err)
		}
		hashes = append(hashes, tx.BlobHashes()...)
	}
	return hashes
}

func requestsOrEmpty(requests [][]byte) []hexutil.Bytes {
	out := make([]hexutil.Bytes, 0, len(requests))
	for _, request := range requests {
		out = append(out, hexutil.Bytes(request))
	}
	return out
}

func dialEngine(ctx context.Context, endpoint, jwtPath string) *rpc.Client {
	jwt, err := node.ObtainJWTSecret(jwtPath)
	if err != nil {
		log.Fatalf("failed to load JWT secret: %v", err)
	}
	var secret [32]byte
	copy(secret[:], jwt)
	client, err := rpc.DialOptions(ctx, endpoint, rpc.WithHTTPAuth(node.NewJWTAuth(secret)))
	if err != nil {
		log.Fatalf("failed to connect to Engine API: %v", err)
	}
	return client
}

func getHeader(ctx context.Context, client *rpc.Client, tag string) *types.Header {
	var header *types.Header
	if err := client.CallContext(ctx, &header, "eth_getBlockByNumber", tag, false); err != nil {
		log.Fatalf("failed to retrieve %s header: %v", tag, err)
	}
	if header == nil {
		log.Fatalf("%s header is unavailable; is geth fully synced?", tag)
	}
	return header
}

func parseHash(name, value string) common.Hash {
	var hash common.Hash
	if err := hash.UnmarshalText([]byte(value)); err != nil {
		log.Fatalf("invalid %s %q: %v", name, value, err)
	}
	return hash
}
