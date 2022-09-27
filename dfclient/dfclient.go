package dfclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
	"github.com/golang/protobuf/ptypes"
	"github.com/sebastianmontero/slog-go/slog"
	"github.com/streamingfast/bstream"
	dfuse "github.com/streamingfast/client-go"
	"github.com/streamingfast/dgrpc"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

var (
	retryDelay   = 5 * time.Second
	reDeltaIndex = regexp.MustCompile(`(.*)__([0-9]+)__([0-9]+)__([0-9]+)`)
	log          *slog.Log
)

// DfClient class, main entry point
type DfClient struct {
	dfuseClient  dfuse.Client
	streamClient pbbstream.BlockStreamV2Client
	decoder      *Decoder
	apiKey       string
}

func (m *DfClient) IsUsingAuthentication() bool {
	return m.apiKey != ""
}

// BlockStreamHandler Should be implemented by any client that wants to process blocks
type BlockStreamHandler interface {
	OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep)
	OnError(err error)
	OnComplete(cursor string, lastBlockRef bstream.BlockRef)
	Cursor(cursor string) string
}

// TableDelta Contains table data data
type TableDelta struct {
	Operation  pbcodec.DBOp_Operation
	Code       string
	Scope      string
	TableName  string
	PrimaryKey string
	OldData    []byte
	NewData    []byte
	DBOp       *pbcodec.DBOp
	Block      *pbcodec.Block
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
	OnDelta(delta *TableDelta, cursor string, forkStep pbbstream.ForkStep)
	OnHeartBeat(block *pbcodec.Block, cursor string)
	OnError(err error)
	OnComplete(lastBlockRef bstream.BlockRef)
}

// DeltaStreamRequest Enables the specification of a delta request
type DeltaStreamRequest struct {
	StartBlockNum      int64
	StartCursor        string
	StopBlockNum       uint64
	ForkSteps          []pbbstream.ForkStep
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
			return fmt.Errorf("Invalid start cursor, no deltaIndex: %v", cursor)
		}

		blockNum, err := strconv.ParseUint(matches[0][2], 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid start cursor, invalid blockNum: %v", cursor)
		}

		traceIndex, err := strconv.Atoi(matches[0][3])
		if err != nil {
			return fmt.Errorf("Invalid start cursor, invalid traceIndex: %v", cursor)
		}
		deltaIndex, err := strconv.Atoi(matches[0][4])
		if err != nil {
			return fmt.Errorf("Invalid start cursor, invalid deltaIndex: %v", cursor)
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
func NewDfClient(dfuseEndpoint, dfuseAPIKey, dfuseAuthURL, chainEndpoint string, logConfig *slog.Config) (*DfClient, error) {
	log = slog.New(logConfig, "dfclient")
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}
	dfuseClientOptions := make([]dfuse.ClientOption, 0)
	if dfuseAPIKey == "" {
		dfuseClientOptions = append(dfuseClientOptions, dfuse.WithoutAuthentication())
	} else if dfuseAuthURL != "" {
		dfuseClientOptions = append(dfuseClientOptions, dfuse.WithAuthURL(dfuseAuthURL))
	}
	dfuseClient, err := dfuse.NewClient(dfuseEndpoint, dfuseAPIKey, dfuseClientOptions...)
	if err != nil {
		log.Error(err, "Unable to create dfuse client")
		return nil, err
	}

	conn, err := dgrpc.NewExternalClient(dfuseEndpoint, dialOptions...)
	if err != nil {
		log.Errorf(err, "Unable to create external gRPC client to: %v", dfuseEndpoint)
		return nil, err
	}

	streamClient := pbbstream.NewBlockStreamV2Client(conn)

	decoder := NewDecoder(chainEndpoint)
	return &DfClient{
		dfuseClient:  dfuseClient,
		streamClient: streamClient,
		decoder:      decoder,
		apiKey:       dfuseAPIKey,
	}, nil
}

// BlockStream enables the streaming of blocks
func (dfClient *DfClient) BlockStream(request *pbbstream.BlocksRequestV2, handler BlockStreamHandler) {
	cursor := ""
	lastBlockRef := bstream.BlockRefEmpty
	log.Infof("Starting a block stream, request: %v", request)
stream:
	for {
		callOptions := make([]grpc.CallOption, 0)
		if dfClient.IsUsingAuthentication() {
			tokenInfo, err := dfClient.dfuseClient.GetAPITokenInfo(context.Background())
			if err != nil {
				log.Error(err, "Unable to retrieve dfuse API token")
				handler.OnError(err)
				return
			}

			credentials := oauth.NewOauthAccess(&oauth2.Token{AccessToken: tokenInfo.Token, TokenType: "Bearer"})
			callOptions = append(callOptions, grpc.PerRPCCredentials(credentials))
		}

		stream, err := dfClient.streamClient.Blocks(context.Background(), request, callOptions...)
		if err != nil {
			log.Error(err, "unable to start blocks stream")
			handler.OnError(err)
			return
		}
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					handler.OnComplete(cursor, lastBlockRef)
					break stream
				}
				request.StartCursor = handler.Cursor(cursor)
				log.Warnf("Stream encountered a remote error, going to retry, cursor: %v, retry delay: %v, err: %v", request.StartCursor, retryDelay, err)
				break
			}

			// logger.Println("Decoding received message's block")
			block := &pbcodec.Block{}
			err = ptypes.UnmarshalAny(response.Block, block)
			if err != nil {
				log.Error(err, "should have been able to unmarshal received block payload")
				handler.OnError(err)
				return
			}
			cursor = response.Cursor
			lastBlockRef = block.AsRef()
			if !block.FilteringApplied || block.FilteredTransactionTraceCount > 0 {
				handler.OnBlock(block, cursor, response.Step)
			}
		}
	}
}

type deltaBlockStreamHandler struct {
	decoder             *Decoder
	request             *DeltaStreamRequest
	handler             DeltaStreamHandler
	countSinceHeartBeat uint
}

func (m *deltaBlockStreamHandler) OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep) {
	reverse := m.request.ReverseUndoOps && forkStep == pbbstream.ForkStep_STEP_UNDO
	traces := block.TransactionTraces()
	if reverse {
		traces = reverseTraces(traces)
	}
	deltaCursor := m.request.cursor
	deltaCursor.BlockNum = uint64(block.Number)
	m.countSinceHeartBeat++
	for traceIndex := deltaCursor.TraceIndex; traceIndex < len(traces); traceIndex++ {
		deltaCursor.TraceIndex = traceIndex
		trace := traces[traceIndex]
		dbOps := trace.DbOps
		if reverse {
			dbOps = reverseDBOps(dbOps)
		}
		for deltaIndex := deltaCursor.DeltaIndex; deltaIndex < len(dbOps); deltaIndex++ {
			dbOp := dbOps[deltaIndex]
			if m.request.HasTable(dbOp.Code, dbOp.TableName) {
				operation := dbOp.Operation
				var oldData, newData []byte
				var err error
				account := eos.AccountName(dbOp.Code)
				table := eos.TableName(dbOp.TableName)
				if dbOp.OldData != nil {
					oldData, err = m.decoder.Decode(account, table, dbOp.OldData)
					if err != nil {
						log.Errorf(err, "Error decoding old data for account: %v, table: %v, blocknum: %v, traceIndex: %v, deltaIndex: %v", account, table, block.Number, traceIndex, deltaIndex)
					}
				}
				if dbOp.NewData != nil {
					newData, err = m.decoder.Decode(account, table, dbOp.NewData)
					if err != nil {
						log.Errorf(err, "Error decoding new data for account: %v, table: %v, blocknum: %v, traceIndex: %v, deltaIndex: %v", account, table, block.Number, traceIndex, deltaIndex)
					}
				}
				if reverse {
					tmp := oldData
					oldData = newData
					newData = tmp
					if operation == pbcodec.DBOp_OPERATION_INSERT {
						operation = pbcodec.DBOp_OPERATION_REMOVE
					} else if operation == pbcodec.DBOp_OPERATION_REMOVE {
						operation = pbcodec.DBOp_OPERATION_INSERT
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

func (m *deltaBlockStreamHandler) OnComplete(cursor string, lastBlockRef bstream.BlockRef) {
	m.handler.OnComplete(lastBlockRef)
}

func (m *deltaBlockStreamHandler) Cursor(cursor string) string {
	return m.request.cursor.BlockCursor
}

// DeltaStream enables the streaming of deltas
func (dfClient *DfClient) DeltaStream(request *DeltaStreamRequest, handler DeltaStreamHandler) {
	contracts := request.Contracts()
	if len(contracts) == 0 {
		handler.OnError(errors.New("At least one table has to be specified in the request"))
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
	dfClient.BlockStream(&pbbstream.BlocksRequestV2{
		StartBlockNum: startBlockNum,
		StartCursor:   cursor.BlockCursor,
		StopBlockNum:  request.StopBlockNum,
		ForkSteps:     request.ForkSteps,
		// IncludeFilterExpr: filter,
		Details: pbbstream.BlockDetails_BLOCK_DETAILS_FULL,
	}, &deltaBlockStreamHandler{
		decoder: dfClient.decoder,
		request: request,
		handler: handler,
	})

}

func reverseTraces(traces []*pbcodec.TransactionTrace) []*pbcodec.TransactionTrace {
	reverse := make([]*pbcodec.TransactionTrace, 0, len(traces))
	for i := len(traces) - 1; i >= 0; i-- {
		reverse = append(reverse, traces[i])
	}
	return reverse
}

func reverseDBOps(dbOps []*pbcodec.DBOp) []*pbcodec.DBOp {
	reverse := make([]*pbcodec.DBOp, 0, len(dbOps))
	for i := len(dbOps) - 1; i >= 0; i-- {
		reverse = append(reverse, dbOps[i])
	}
	return reverse
}
