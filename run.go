package main

import (
	"fmt"

	pbantelope "github.com/pinax-network/firehose-antelope/types/pb/sf/antelope/type/v1"
	"github.com/rs/zerolog"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
	"github.com/sebastianmontero/slog-go/slog"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
)

type blockStreamHandler struct {
	cursor string
}

func (m *blockStreamHandler) OnBlock(block *pbantelope.Block, cursor string, forkStep pbfirehose.ForkStep) {
	// fmt.Println("On Block: ", block, "Cursor: ", cursor, "Fork Step:", forkStep)
	fmt.Println("Cursor: ", cursor, "Fork Step:", forkStep)
}

func (m *blockStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (m *blockStreamHandler) OnComplete(cursor string) {
	fmt.Println("On Complete cursor: ", cursor)
}

func (m *blockStreamHandler) Cursor(cursor string) string {
	return cursor
}

type deltaStreamHandler struct {
	cursor string
}

func (handler *deltaStreamHandler) OnDelta(delta *dfclient.TableDelta, cursor string, forkStep pbfirehose.ForkStep) {
	fmt.Println("Cursor: ", cursor, "Fork Step:", forkStep, "\nOn Delta: ", delta)
}

func (handler *deltaStreamHandler) OnHeartBeat(block *pbantelope.Block, cursor string) {
	fmt.Println("On Heartbeat, block num: ", block.Number, " cursor: ", cursor)
}

func (handler *deltaStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (handler *deltaStreamHandler) OnComplete() {
	fmt.Println("On Complete")
}

func main() {
	dfuseEndpoint := "telos.firehose.pinax.network:443"
	dfuseJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2FwaS5hY2NvdW50LnBpbmF4Lm5ldHdvcmsvdjEvIiwic3ViIjoiZDllMzYwNTUtZDI3OS00ZTQ2LTgwYzgtNjhlYjQ1NmFmYTAyIiwiYXVkIjpbImh0dHBzOi8vYWNjb3VudC5waW5heC5uZXR3b3JrLyJdLCJleHAiOjIwMjA3NzY2ODIsImlhdCI6MTcwNTQxNjY4MiwiYXBpX2tleV9pZCI6ImI3M2JlMjVmLWMyZjctNDlkZC1iZjA4LWEyMDRkZTc5ZTg5ZCJ9.Qdkm1n7yJP7GxHMDL66yfIS7Kr588wfKUasQhYfBBhc"

	client, err := dfclient.NewDfClient(dfuseEndpoint, dfuseJWT, &slog.Config{Pretty: true, Level: zerolog.TraceLevel})

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
		StartBlockNum: 320030296,
		// StartCursor:    "-NZ02QtqUc65KeC9HlF3Q6WwLpcyB11tXQPmLRREj4un9CaTi5_0AmUgPE_Ywfuj3BfoQl-s2NebQHd888FV6tS5lrw163Q_T3wsktrt-OLsLfr3OA0TcuhkDuuMY9DRWjvVagL4frAJ6tW2PqePMxZgMMcvJDe1h2pWpdFccaMX63c9yjr4J8eA0aiV9oQUrbMsEOXzx3qmVmYof04POsSLbvHK6mp2Z3E=__172883755__0__0",
		StopBlockNum:    0,
		FinalBlocksOnly: false,
		ReverseUndoOps:  true,
	}
	// deltaRequest.AddTables("eosio", []string{"payments"})
	deltaRequest.AddTables("dao.hypha", []string{"documents", "edges"})
	client.DeltaStream(deltaRequest, &deltaStreamHandler{})
}
