package main

import (
	"fmt"

	"github.com/dfuse-io/bstream"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
)

type blockStreamHandler struct {
	cursor string
}

func (handler *blockStreamHandler) OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep) {
	fmt.Println("On Block: ", block, "Cursor: ", cursor, "Fork Step:", forkStep)
}

func (handler *blockStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (handler *blockStreamHandler) OnComplete(cursor string, lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete cursor: ", cursor, "Last Block Ref: ", lastBlockRef)
}

type deltaStreamHandler struct {
	cursor string
}

func (handler *deltaStreamHandler) OnDelta(delta *dfclient.TableDelta, cursor string, forkStep pbbstream.ForkStep) {
	fmt.Println("On Delta: ", delta.DBOp, "Cursor: ", cursor, "Fork Step:", forkStep)
}

func (handler *deltaStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (handler *deltaStreamHandler) OnComplete(lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete Last Block Ref: ", lastBlockRef)
}

func main() {
	endpoint := "test.telos.kitchen:9000"
	apiKey := "server_eeb2882943ae420bfb3eb9bf3d78ed9d"
	client, err := dfclient.NewDfClient(endpoint, apiKey)

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
		StartBlockNum:  87822500,
		StartCursor:    "ARHsGvAmFIfc4SA5svnskczBJJM7VVtrXVjjIxdA09mi8CbN35yuCGFxbUjXma6i20DuSVOt1ovFEigu9JQDu4TvxrFt5CBuFyoqlormr7y8etNOoMoC__0__0",
		StopBlockNum:   87823501,
		ForkSteps:      []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
		ReverseUndoOps: true,
	}
	// deltaRequest.AddTables("eosio.token", []string{"balance"})
	deltaRequest.AddTables("eosio", []string{"payments", "producers"})
	client.DeltaStream(deltaRequest, &deltaStreamHandler{})
}
