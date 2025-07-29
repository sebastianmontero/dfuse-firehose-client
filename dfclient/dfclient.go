package dfclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/mostynb/go-grpc-compression/zstd"
	"google.golang.org/grpc"

	pbantelope "buf.build/gen/go/pinax/firehose-antelope/protocolbuffers/go/sf/antelope/type/v1"
	"github.com/sebastianmontero/slog-go/slog"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"

	"github.com/streamingfast/firehose-core/firehose/client"
)

var (
	retryDelay   = 5 * time.Second
	reDeltaIndex = regexp.MustCompile(`(.*)__([0-9]+)__([0-9]+)__([0-9]+)`)
	log          *slog.Log
)

// DfClient class, main entry point
type DfClient struct {
	streamClient pbfirehose.StreamClient
	closeFunc    func() error
	callOpts     []grpc.CallOption
}

// BlockStreamHandler Should be implemented by any client that wants to process blocks
type BlockStreamHandler interface {
	OnBlock(block *pbantelope.Block, cursor string, forkStep pbfirehose.ForkStep)
	OnError(err error)
	OnComplete(cursor string)
	Cursor(cursor string) string
}

// TableDelta Contains table data data
type TableDelta struct {
	Operation  pbantelope.DBOp_Operation
	Code       string
	Scope      string
	TableName  string
	PrimaryKey string
	OldData    string
	NewData    string
	DBOp       *pbantelope.DBOp
	Block      *pbantelope.Block
}

func (m *TableDelta) String() string {
	return fmt.Sprintf(
		"Op: %v, Code: %v, Scope: %v, TableName: %v, PrimaryKey: %v\nOldData: %v\nNewData: %v",
		m.Operation,
		m.Code,
		m.Scope,
		m.TableName,
		m.PrimaryKey,
		string(m.OldData),
		string(m.NewData),
	)
}

// DeltaStreamHandler Should be implemented by any client that wants to process table deltas
type DeltaStreamHandler interface {
	OnDelta(delta *TableDelta, cursor string, forkStep pbfirehose.ForkStep)
	OnHeartBeat(block *pbantelope.Block, cursor string)
	OnError(err error)
	OnComplete()
}

// DeltaStreamRequest Enables the specification of a delta request
type DeltaStreamRequest struct {
	StartBlockNum      int64
	StartCursor        string
	StopBlockNum       uint64
	FinalBlocksOnly    bool
	ReverseUndoOps     bool
	HeartBeatFrequency uint
	tables             map[string]map[string]bool
	cursor             *DeltaCursor
}

// ParseCursor parses startCursor and creates a new DeltaCursor
func (m *DeltaStreamRequest) ParseCursor() (*DeltaCursor, error) {
	cursor, err := NewDeltaCursor(m.StartCursor)
	if err != nil {
		return nil, err
	}
	m.cursor = cursor
	return cursor, nil
}

// AddTables Adds tables for a specific contract to the delta request
func (m *DeltaStreamRequest) AddTables(contract string, tables []string) {
	if m.tables == nil {
		m.tables = make(map[string]map[string]bool)
	}
	contractMap, ok := m.tables[contract]
	if !ok {
		contractMap = make(map[string]bool)
		m.tables[contract] = contractMap
	}
	for _, table := range tables {
		contractMap[table] = true
	}
}

// HasTable Checks if a table was requested
func (m *DeltaStreamRequest) HasTable(contract string, table string) bool {

	if contractMap, ok := m.tables[contract]; ok {
		_, ok := contractMap[table]
		return ok
	}
	return false
}

// Contracts Returns contracts with requested tables
func (m *DeltaStreamRequest) Contracts() []string {
	contracts := make([]string, 0, len(m.tables))

	for contract, contractMap := range m.tables {
		if len(contractMap) > 0 {
			contracts = append(contracts, contract)
		}
	}
	return contracts
}

// DeltaCursor stores delta cursor information
type DeltaCursor struct {
	BlockCursor string
	BlockNum    uint64
	TraceIndex  int
	DeltaIndex  int
}

// NewDeltaCursor creates a new cursor
func NewDeltaCursor(cursor string) (*DeltaCursor, error) {
	deltaCursor := &DeltaCursor{}
	err := deltaCursor.Update(cursor)
	return deltaCursor, err
}

// Update parses a string cursor into its different components
func (m *DeltaCursor) Update(cursor string) error {
	if cursor != "" {
		matches := reDeltaIndex.FindAllStringSubmatch(cursor, -1)
		if len(matches) == 0 {
			return fmt.Errorf("invalid start cursor, no deltaIndex: %v", cursor)
		}

		blockNum, err := strconv.ParseUint(matches[0][2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid start cursor, invalid blockNum: %v", cursor)
		}

		traceIndex, err := strconv.Atoi(matches[0][3])
		if err != nil {
			return fmt.Errorf("invalid start cursor, invalid traceIndex: %v", cursor)
		}
		deltaIndex, err := strconv.Atoi(matches[0][4])
		if err != nil {
			return fmt.Errorf("invalid start cursor, invalid deltaIndex: %v", cursor)
		}
		m.BlockCursor = matches[0][1]
		m.BlockNum = blockNum
		m.TraceIndex = traceIndex
		m.DeltaIndex = deltaIndex
	}
	return nil
}

// String returns a string representation of the cursor
func (m *DeltaCursor) String() string {
	return m.BlockCursor + "__" + strconv.FormatUint(m.BlockNum, 10) + "__" + strconv.Itoa(m.TraceIndex) + "__" + strconv.Itoa(m.DeltaIndex)
}

// HasBlockNum indicates whether the cursor has the blockNum set
func (m *DeltaCursor) HasBlockNum() bool {
	return m.BlockNum != 0
}

// NewDfClient DfClient constructor
func NewDfClient(dfuseEndpoint, apiKey string, logConfig *slog.Config) (*DfClient, error) {
	log = slog.New(logConfig, "dfclient")

	// Create a new Firehose stream client to connect to the infrastructure. The parameters set here are set for our
	// public endpoints.
	//
	// In case you are running a Firehose node yourself, you might want to set useInsecureTLSConnection or use
	// PlainTextConnection depending on whether you are using self-signed TLS certificates or non-TLS connections.
	fhClient, closeFunc, callOpts, err := client.NewFirehoseClient(dfuseEndpoint, "", apiKey, false, false)
	if err != nil {
		log.Panic(err, "failed to create Firehose client")
	}

	// Optionally you can enable gRPC compression
	callOpts = append(callOpts, grpc.UseCompressor(zstd.Name))

	return &DfClient{
		streamClient: fhClient,
		closeFunc:    closeFunc,
		callOpts:     callOpts,
	}, nil
}

// BlockStream enables the streaming of blocks
func (dfClient *DfClient) BlockStream(request *pbfirehose.Request, handler BlockStreamHandler) {
	cursor := ""
	log.Infof("Starting a block stream, request: %v", request)
	defer dfClient.closeFunc()
stream:
	for {
		stream, err := dfClient.streamClient.Blocks(context.Background(), request, dfClient.callOpts...)
		if err != nil {
			log.Error(err, "unable to start blocks stream")
			handler.OnError(err)
			return
		}
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					handler.OnComplete(cursor)
					break stream
				}
				request.Cursor = handler.Cursor(cursor)
				log.Warnf("Stream encountered a remote error, going to retry, cursor: %v, retry delay: %v, err: %v", request.Cursor, retryDelay, err)
				break
			}

			var block pbantelope.Block
			err = response.Block.UnmarshalTo(&block)
			if err != nil {
				log.Error(err, "should have been able to unmarshal received block payload")
				handler.OnError(err)
				return
			}
			cursor = response.Cursor
			if !block.FilteringApplied || block.FilteredTransactionTraceCount > 0 {
				handler.OnBlock(&block, cursor, response.Step)
			}
		}
	}
}

type deltaBlockStreamHandler struct {
	request             *DeltaStreamRequest
	handler             DeltaStreamHandler
	countSinceHeartBeat uint
}

func (m *deltaBlockStreamHandler) OnBlock(block *pbantelope.Block, cursor string, forkStep pbfirehose.ForkStep) {
	reverse := m.request.ReverseUndoOps && forkStep == pbfirehose.ForkStep_STEP_UNDO
	traces := block.GetUnfilteredTransactionTraces()
	if reverse {
		traces = reverseTraces(traces)
	}
	deltaCursor := m.request.cursor
	deltaCursor.BlockNum = uint64(block.Number)
	// fmt.Println("On Block: ", block, "Cursor: ", cursor, "Fork Step:", forkStep)
	m.countSinceHeartBeat++
	for traceIndex := deltaCursor.TraceIndex; traceIndex < len(traces); traceIndex++ {
		deltaCursor.TraceIndex = traceIndex
		trace := traces[traceIndex]
		dbOps := trace.DbOps
		// fmt.Println("On Trace: ", traceIndex, "Cursor: ", cursor, "Fork Step:", forkStep, "DBOps: ", len(dbOps))
		if reverse {
			dbOps = reverseDBOps(dbOps)
		}
		for deltaIndex := deltaCursor.DeltaIndex; deltaIndex < len(dbOps); deltaIndex++ {
			dbOp := dbOps[deltaIndex]
			// fmt.Println("On Delta: ", deltaIndex, "Cursor: ", cursor, "Fork Step:", forkStep, "DBOp: ", dbOp)
			if m.request.HasTable(dbOp.Code, dbOp.TableName) {
				operation := dbOp.Operation
				oldData := dbOp.OldDataJson
				newData := dbOp.NewDataJson
				if reverse {
					tmp := oldData
					oldData = newData
					newData = tmp
					if operation == pbantelope.DBOp_OPERATION_INSERT {
						operation = pbantelope.DBOp_OPERATION_REMOVE
					} else if operation == pbantelope.DBOp_OPERATION_REMOVE {
						operation = pbantelope.DBOp_OPERATION_INSERT
					}
				}
				deltaCursor.DeltaIndex = deltaIndex + 1
				m.request.StartCursor = deltaCursor.String()
				m.handler.OnDelta(&TableDelta{
					Operation:  operation,
					Code:       dbOp.Code,
					Scope:      dbOp.Scope,
					TableName:  dbOp.TableName,
					PrimaryKey: dbOp.PrimaryKey,
					OldData:    oldData,
					NewData:    newData,
					DBOp:       dbOp,
					Block:      block,
				},
					m.request.StartCursor,
					forkStep)
				m.countSinceHeartBeat = 0 //Deltas are considered heart beats
			}
		}
		deltaCursor.DeltaIndex = 0
	}
	deltaCursor.TraceIndex = 0
	deltaCursor.BlockCursor = cursor
	m.request.StartCursor = deltaCursor.String()
	if m.countSinceHeartBeat > m.request.HeartBeatFrequency {
		m.handler.OnHeartBeat(block, m.request.StartCursor)
		m.countSinceHeartBeat = 0
	}
}

func (m *deltaBlockStreamHandler) OnError(err error) {
	m.handler.OnError(err)
}

func (m *deltaBlockStreamHandler) OnComplete(cursor string) {
	m.handler.OnComplete()
}

func (m *deltaBlockStreamHandler) Cursor(cursor string) string {
	return m.request.cursor.BlockCursor
}

// DeltaStream enables the streaming of deltas
func (dfClient *DfClient) DeltaStream(request *DeltaStreamRequest, handler DeltaStreamHandler) {
	contracts := request.Contracts()
	if len(contracts) == 0 {
		handler.OnError(errors.New("at least one table has to be specified in the request"))
		return
	}
	cursor, err := request.ParseCursor()
	if err != nil {
		handler.OnError(err)
		return
	}
	var startBlockNum int64
	if cursor.HasBlockNum() {
		startBlockNum = int64(cursor.BlockNum)
	} else {
		startBlockNum = request.StartBlockNum
	}
	// filter := "receiver in ['" + strings.Join(contracts, "','") + "']"
	dfClient.BlockStream(&pbfirehose.Request{
		StartBlockNum:   startBlockNum,
		Cursor:          cursor.BlockCursor,
		StopBlockNum:    request.StopBlockNum,
		FinalBlocksOnly: request.FinalBlocksOnly,
	}, &deltaBlockStreamHandler{
		request: request,
		handler: handler,
	})

}

func reverseTraces(traces []*pbantelope.TransactionTrace) []*pbantelope.TransactionTrace {
	reverse := make([]*pbantelope.TransactionTrace, 0, len(traces))
	for i := len(traces) - 1; i >= 0; i-- {
		reverse = append(reverse, traces[i])
	}
	return reverse
}

func reverseDBOps(dbOps []*pbantelope.DBOp) []*pbantelope.DBOp {
	reverse := make([]*pbantelope.DBOp, 0, len(dbOps))
	for i := len(dbOps) - 1; i >= 0; i-- {
		reverse = append(reverse, dbOps[i])
	}
	return reverse
}
