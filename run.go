package main

import (
	"fmt"

	"github.com/dfuse-io/bstream"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/rs/zerolog"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
)

type blockStreamHandler struct {
	cursor string
}

func (m *blockStreamHandler) OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep) {
	fmt.Println("On Block: ", block, "Cursor: ", cursor, "Fork Step:", forkStep)
}

func (m *blockStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (m *blockStreamHandler) OnComplete(cursor string, lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete cursor: ", cursor, "Last Block Ref: ", lastBlockRef)
}

func (m *blockStreamHandler) Cursor(cursor string) string {
	return cursor
}

type deltaStreamHandler struct {
	cursor string
}

func (handler *deltaStreamHandler) OnDelta(delta *dfclient.TableDelta, cursor string, forkStep pbbstream.ForkStep) {
	fmt.Println("Cursor: ", cursor, "Fork Step:", forkStep, "\nOn Delta: ", delta)
}

func (handler *deltaStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (handler *deltaStreamHandler) OnComplete(lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete Last Block Ref: ", lastBlockRef)
}

func main() {
	dfuseEndpoint := "localhost:9000"
	dfuseAPIKey := "server_eeb2882943ae420bfb3eb9bf3d78ed9d"
	chainEndpoint := "https://testnet.telos.caleos.io"
	client, err := dfclient.NewDfClient(dfuseEndpoint, dfuseAPIKey, chainEndpoint, zerolog.TraceLevel)

	if err != nil {
		panic(fmt.Sprintln("Error creating client: ", err))
	}
	// client.BlockStream(&pbbstream.BlocksRequestV2{
	// 	StartBlockNum:     87822500,
	// 	StartCursor:       "",
	// 	StopBlockNum:      87823501,
	// 	ForkSteps:         []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE},
	// 	IncludeFilterExpr: "receiver in ['eosio.token']",
	// 	Details:           pbbstream.BlockDetails_BLOCK_DETAILS_FULL,
	// }, &blockStreamHandler{})
	deltaRequest := &dfclient.DeltaStreamRequest{
		StartBlockNum:  87993300,
		StartCursor:    "",
		StopBlockNum:   0,
		ForkSteps:      []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
		ReverseUndoOps: true,
	}
	// deltaRequest.AddTables("eosio", []string{"payments"})
	deltaRequest.AddTables("dao.hypha", []string{"documents", "edges"})
	client.DeltaStream(deltaRequest, &deltaStreamHandler{})
}
