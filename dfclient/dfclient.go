package dfclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/dfuse-io/bstream"
	dfuse "github.com/dfuse-io/client-go"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/dfuse-io/dgrpc"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

var (
	logger       *log.Logger = log.New(os.Stdout, "dfClient", log.Lshortfile)
	retryDelay               = 5 * time.Second
	reDeltaIndex             = regexp.MustCompile(`(.+)__([0-9]+)__([0-9]+)`)
)

//DfClient class, main entry point
type DfClient struct {
	dfuseClient  dfuse.Client
	streamClient pbbstream.BlockStreamV2Client
}

//BlockStreamHandler Should be implemented by any client that wants to process blocks
type BlockStreamHandler interface {
	OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep)
	OnError(err error)
	OnComplete(cursor string, lastBlockRef bstream.BlockRef)
}

//TableDelta Contains table data data
type TableDelta struct {
	Operation  pbcodec.DBOp_Operation
	Code       string
	Scope      string
	TableName  string
	PrimaryKey string
	DBOp       *pbcodec.DBOp
	Block      *pbcodec.Block
}

//DeltaStreamHandler Should be implemented by any client that wants to process table deltas
type DeltaStreamHandler interface {
	OnDelta(delta *TableDelta, cursor string, forkStep pbbstream.ForkStep)
	OnError(err error)
	OnComplete(lastBlockRef bstream.BlockRef)
}

//DeltaStreamRequest Enables the specification of a delta request
type DeltaStreamRequest struct {
	StartBlockNum  int64
	StartCursor    string
	StopBlockNum   uint64
	ForkSteps      []pbbstream.ForkStep
	ReverseUndoOps bool
	tables         map[string]map[string]bool
	blockCursor    string
	traceIndex     int
	deltaIndex     int
}

//AddTables Adds tables for a specific contract to the delta request
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

//HasTable Checks if a table was requested
func (m *DeltaStreamRequest) HasTable(contract string, table string) bool {

	if contractMap, ok := m.tables[contract]; ok {
		_, ok := contractMap[table]
		return ok
	}
	return false
}

//Contracts Returns contracts with requested tables
func (m *DeltaStreamRequest) Contracts() []string {
	contracts := make([]string, 0, len(m.tables))

	for contract, contractMap := range m.tables {
		if len(contractMap) > 0 {
			contracts = append(contracts, contract)
		}
	}
	return contracts
}

//ParseCursor Separates start cursor into block cursor, traceIndex and deltaIndex
func (m *DeltaStreamRequest) ParseCursor() error {
	if m.StartCursor != "" {
		matches := reDeltaIndex.FindAllStringSubmatch(m.StartCursor, -1)
		if len(matches) == 0 {
			return fmt.Errorf("Invalid start cursor, no deltaIndex: %v", m.StartCursor)
		}

		traceIndex, err := strconv.Atoi(matches[0][2])
		if err != nil {
			return fmt.Errorf("Invalid start cursor, invalid traceIndex: %v", m.StartCursor)
		}
		deltaIndex, err := strconv.Atoi(matches[0][3])
		if err != nil {
			return fmt.Errorf("Invalid start cursor, invalid deltaIndex: %v", m.StartCursor)
		}
		m.blockCursor = matches[0][1]
		m.traceIndex = traceIndex
		m.deltaIndex = deltaIndex
	}
	return nil
}

// UpdateCursor updates cursor info
func (m *DeltaStreamRequest) UpdateCursor(blockCursor string, traceIndex int, deltaIndex int) {
	m.blockCursor = blockCursor
	m.traceIndex = traceIndex
	m.deltaIndex = deltaIndex
	m.StartCursor = blockCursor + "__" + strconv.Itoa(traceIndex) + "__" + strconv.Itoa(deltaIndex)
}

//NewDfClient DfClient constructor
func NewDfClient(endpoint string, apiKey string) (*DfClient, error) {
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}

	dfuseClient, err := dfuse.NewClient(endpoint, apiKey)
	if err != nil {
		logger.Println("Unable to create dfuse client")
		return nil, err
	}

	conn, err := dgrpc.NewExternalClient(endpoint, dialOptions...)
	if err != nil {
		logger.Printf("Unable to create external gRPC client to %q", endpoint)
		return nil, err
	}

	streamClient := pbbstream.NewBlockStreamV2Client(conn)
	return &DfClient{dfuseClient, streamClient}, nil
}

//BlockStream enables the streaming of blocks
func (dfClient *DfClient) BlockStream(request *pbbstream.BlocksRequestV2, handler BlockStreamHandler) {
	cursor := ""
	lastBlockRef := bstream.BlockRefEmpty
	logger.Println("Starting a block stream, request: ", request)
stream:
	for {
		tokenInfo, err := dfClient.dfuseClient.GetAPITokenInfo(context.Background())
		if err != nil {
			logger.Println("unable to retrieve dfuse API token")
			handler.OnError(err)
			return
		}

		credentials := oauth.NewOauthAccess(&oauth2.Token{AccessToken: tokenInfo.Token, TokenType: "Bearer"})
		stream, err := dfClient.streamClient.Blocks(context.Background(), request, grpc.PerRPCCredentials(credentials))
		if err != nil {
			logger.Println("unable to start blocks stream")
			handler.OnError(err)
			return
		}

		for {
			// logger.Println("Waiting for message to reach us")
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					handler.OnComplete(cursor, lastBlockRef)
					break stream
				}
				logger.Printf("Stream encountered a remote error, going to retry, cursor: %v, last block: %v, retry delay: %v, err: %v", cursor, lastBlockRef, retryDelay, err)
				break
			}

			// logger.Println("Decoding received message's block")
			block := &pbcodec.Block{}
			err = ptypes.UnmarshalAny(response.Block, block)
			if err != nil {
				logger.Println("should have been able to unmarshal received block payload")
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
	Request *DeltaStreamRequest
	Handler DeltaStreamHandler
}

func (m *deltaBlockStreamHandler) OnBlock(block *pbcodec.Block, cursor string, forkStep pbbstream.ForkStep) {
	reverse := m.Request.ReverseUndoOps && forkStep == pbbstream.ForkStep_STEP_UNDO
	traces := block.TransactionTraces()
	for traceIndex, trace := range traces {
		dbOps := trace.DbOps
		for dbOpIndex, dbOp := range dbOps {
			if m.Request.HasTable(dbOp.Code, dbOp.TableName) {
				operation := dbOp.Operation
				if reverse {
					if operation == pbcodec.DBOp_OPERATION_INSERT {
						operation = pbcodec.DBOp_OPERATION_REMOVE
					} else if operation == pbcodec.DBOp_OPERATION_REMOVE {
						operation = pbcodec.DBOp_OPERATION_INSERT
					}
				}
				m.Request.UpdateCursor(cursor, traceIndex, dbOpIndex)
				m.Handler.OnDelta(&TableDelta{
					Operation:  operation,
					Code:       dbOp.Code,
					Scope:      dbOp.Scope,
					TableName:  dbOp.TableName,
					PrimaryKey: dbOp.PrimaryKey,
					DBOp:       dbOp,
					Block:      block,
				},
					m.Request.StartCursor,
					forkStep)
			}
		}

	}
}

func (m *deltaBlockStreamHandler) OnError(err error) {
	m.Handler.OnError(err)
}

func (m *deltaBlockStreamHandler) OnComplete(cursor string, lastBlockRef bstream.BlockRef) {
	m.Handler.OnComplete(lastBlockRef)
}

//DeltaStream enables the streaming of deltas
func (dfClient *DfClient) DeltaStream(request *DeltaStreamRequest, handler DeltaStreamHandler) {
	contracts := request.Contracts()
	if len(contracts) == 0 {
		handler.OnError(errors.New("At least one table has to be specified in the request"))
		return
	}
	err := request.ParseCursor()
	if err != nil {
		handler.OnError(err)
		return
	}
	// filter := "receiver in ['" + strings.Join(contracts, "','") + "']"
	dfClient.BlockStream(&pbbstream.BlocksRequestV2{
		StartBlockNum: request.StartBlockNum,
		StartCursor:   request.blockCursor,
		StopBlockNum:  request.StopBlockNum,
		ForkSteps:     request.ForkSteps,
		// IncludeFilterExpr: filter,
		Details: pbbstream.BlockDetails_BLOCK_DETAILS_FULL,
	}, &deltaBlockStreamHandler{
		Request: request,
		Handler: handler,
	})

}
