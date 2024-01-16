package main

import (
	"fmt"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/rs/zerolog"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
	"github.com/sebastianmontero/slog-go/slog"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
)

type blockStreamHandler struct {
	cursor string
}

func (m *blockStreamHandler) OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep) {
	// fmt.Println("On Block: ", block, "Cursor: ", cursor, "Fork Step:", forkStep)
	fmt.Println("Cursor: ", cursor, "Fork Step:", forkStep)
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

func (handler *deltaStreamHandler) OnHeartBeat(block *pbcodec.Block, cursor string) {
	fmt.Println("On Heartbeat, block num: ", block.Number, " cursor: ", cursor)
}

func (handler *deltaStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (handler *deltaStreamHandler) OnComplete(lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete Last Block Ref: ", lastBlockRef)
}

func main() {
	dfuseEndpoint := "telos.firehose.eosnation.io:9000"
	dfuseAPIKey := "dc6087c88050f3caeed46f22767c357c"
	dfuseAuthURL := "https://auth.eosnation.io"
	chainEndpoint := "https://mainnet.telos.net"

	client, err := dfclient.NewDfClient(dfuseEndpoint, dfuseAPIKey, dfuseAuthURL, chainEndpoint, &slog.Config{Pretty: true, Level: zerolog.TraceLevel})

	if err != nil {
		panic(fmt.Sprintln("Error creating client: ", err))
	}
	// client.BlockStream(&pbbstream.BlocksRequestV2{
	// 	StartBlockNum: 100450000,

	// 	// StartCursor:  "Wjf2BxOOdnd-hvrA28BySaWwLpcyB15sVw_mLBVGj4v--HuX1JrwVDN1bRSFwqr3jxS_Qgn_3YrMQnsuo8UFu9XqkL5g5HM_RH8ll4jsqb3vKvf6OFhKcek0WL_fNtzRWzY=",
	// 	StopBlockNum: 0,
	// 	// ForkSteps:     []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE},

	// 	ForkSteps: []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
	// 	// IncludeFilterExpr: "receiver in ['eosio.token']",
	// 	// Details: pbbstream.BlockDetails_BLOCK_DETAILS_FULL,
	// }, &blockStreamHandler{})
	deltaRequest := &dfclient.DeltaStreamRequest{
		StartBlockNum: 242834360,
		// StartCursor:    "-NZ02QtqUc65KeC9HlF3Q6WwLpcyB11tXQPmLRREj4un9CaTi5_0AmUgPE_Ywfuj3BfoQl-s2NebQHd888FV6tS5lrw163Q_T3wsktrt-OLsLfr3OA0TcuhkDuuMY9DRWjvVagL4frAJ6tW2PqePMxZgMMcvJDe1h2pWpdFccaMX63c9yjr4J8eA0aiV9oQUrbMsEOXzx3qmVmYof04POsSLbvHK6mp2Z3E=__172883755__0__0",
		StopBlockNum:   0,
		ForkSteps:      []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
		ReverseUndoOps: true,
	}
	// deltaRequest.AddTables("eosio", []string{"payments"})
	deltaRequest.AddTables("dao.hypha", []string{"documents", "edges"})
	client.DeltaStream(deltaRequest, &deltaStreamHandler{})
}
