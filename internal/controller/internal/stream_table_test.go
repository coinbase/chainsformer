package internal

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	csmocks "github.com/coinbase/chainsformer/internal/chainstorage/mocks"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
	sdkmocks "github.com/coinbase/chainstorage/sdk/mocks"

	"github.com/coinbase/chainsformer/internal/config"
	internalerrors "github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/partition"
	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	"github.com/coinbase/chainsformer/internal/utils/testapp"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	xarrowmocks "github.com/coinbase/chainsformer/internal/utils/xarrow/mocks"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

type (
	testStreamedBlocksTable struct{}

	testMocks struct {
		ctrl          *gomock.Controller
		session       *csmocks.MockSession
		client        *sdkmocks.MockClient
		parser        *sdkmocks.MockParser
		tableWriter   *xarrowmocks.MockTableWriter
		recordBuilder *array.RecordBuilder
		streamTable   *StreamTable
	}

	streamTableTestSuite struct {
		suite.Suite
	}

	miniBatchInfo struct {
		sequenceNumber int64
		miniBatchSize  uint64
	}
)

var (
	failedToGetStorageStartSequenceError = xerrors.New("failed to get storage start sequence")
	failedToGetStorageEndSequenceError   = xerrors.New("failed to get storage end sequence")
	failedToGetChainEventError           = xerrors.New("failed to get chain events")
	failedToGetBlockWithTagError         = xerrors.New("failed to get block with tag")
)

func TestStreamTableTestSuite(t *testing.T) {
	suite.Run(t, new(streamTableTestSuite))
}

func (s *streamTableTestSuite) TestGetEndpoints_NotProvideGetStreamFlightInfoCommandShouldReturnError() {
	testMocks := newTestMocks(s.T())
	_, err := testMocks.streamTable.GetEndpoints(context.Background(), nil)
	assert.True(s.T(), errors.Is(err, internalerrors.ErrInvalidArgument))
}

func (s *streamTableTestSuite) TestGetEndpoints() {
	testCases := map[string]struct {
		testMocks                    testMocks
		requestStartSequence         int64
		requestEndSequence           int64
		requestEventsPerPartition    uint64
		storageStartSequence         int64
		getStorageStartSequenceError error
		storageEndSequence           int64
		getStorageEndSequenceError   error
		expectedOutputError          error
	}{
		"request start and end sequence within storage range and default number of events per partition returns a single ticket": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 1,
			requestEndSequence:   2,
			storageStartSequence: 0,
			storageEndSequence:   10,
		},

		"request start less than storage start sequence and default number of events per partition clamps the ticket start sequence": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 0,
			requestEndSequence:   3,
			storageStartSequence: 1,
			storageEndSequence:   10,
		},

		"request end larger than storage end sequence and default number of events per partition clamps the ticket end sequence": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 1,
			requestEndSequence:   12,
			storageStartSequence: 0,
			storageEndSequence:   10,
		},

		"request start and end sequence within storage range with multiple partitions returns a multiple tickets with equal number of events": {
			testMocks:                 newTestMocks(s.T()),
			requestStartSequence:      1,
			requestEndSequence:        11,
			requestEventsPerPartition: 2,
			storageStartSequence:      0,
			storageEndSequence:        10,
		},

		"request start and end sequence within storage range with multiple partitions returns a multiple tickets with unequal number of events in the final ticket": {
			testMocks:                 newTestMocks(s.T()),
			requestStartSequence:      0,
			requestEndSequence:        9,
			requestEventsPerPartition: 2,
			storageStartSequence:      0,
			storageEndSequence:        10,
		},

		"request end sequence being 0 and default number of events per partition returns one ticket with the entire range": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 1,
			storageStartSequence: 0,
			storageEndSequence:   10,
		},

		"request start sequence larger than storage end sequence returns invalid arguments error": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 11,
			requestEndSequence:   12,
			storageStartSequence: 0,
			storageEndSequence:   10,
			expectedOutputError:  internalerrors.ErrInvalidArgument,
		},

		"request end sequence larger than 0 but equal to request start sequence returns invalid arguments error": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 2,
			requestEndSequence:   2,
			storageStartSequence: 0,
			storageEndSequence:   10,
			expectedOutputError:  internalerrors.ErrInvalidArgument,
		},

		"failed to get storage start sequence returns internal error": {
			testMocks:                    newTestMocks(s.T()),
			requestStartSequence:         1,
			requestEndSequence:           2,
			storageStartSequence:         0,
			getStorageStartSequenceError: failedToGetStorageStartSequenceError,
			expectedOutputError:          failedToGetStorageStartSequenceError,
		},

		"failed to get storage end sequence returns internal error": {
			testMocks:                  newTestMocks(s.T()),
			requestStartSequence:       1,
			requestEndSequence:         2,
			storageStartSequence:       0,
			getStorageEndSequenceError: failedToGetStorageEndSequenceError,
			expectedOutputError:        failedToGetStorageEndSequenceError,
		},
	}

	for testName, tc := range testCases {
		tc := tc
		s.T().Run(testName, func(t *testing.T) {
			tc.testMocks.session.EXPECT().
				GetEventSequenceByPosition(gomock.Any(), chainstorage.EarliestEventPosition).
				Return(tc.storageStartSequence, tc.getStorageStartSequenceError)

			if tc.getStorageStartSequenceError == nil {
				tc.testMocks.session.EXPECT().
					GetEventSequenceByPosition(gomock.Any(), chainstorage.LatestEventPosition).
					Return(tc.storageEndSequence, tc.getStorageEndSequenceError)
			}

			cmd := &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						StartSequence:      tc.requestStartSequence,
						EndSequence:        tc.requestEndSequence,
						EventsPerPartition: tc.requestEventsPerPartition,
					},
				},
			}

			endpoints, err := tc.testMocks.streamTable.GetEndpoints(context.Background(), cmd)
			assert.True(s.T(), errors.Is(err, tc.expectedOutputError))
			if err == nil {
				expectedEndpoints, err := getExpectedFlightEndpoints(tc.requestStartSequence, tc.requestEndSequence, tc.requestEventsPerPartition, tc.storageStartSequence, tc.storageEndSequence)
				s.Require().Nil(err)
				s.Require().Equal(len(expectedEndpoints), len(endpoints))

				for i, expectedEndpoint := range expectedEndpoints {
					s.Require().EqualValues(*expectedEndpoint.GetTicket(), *endpoints[i].GetTicket())
				}
			}
		})
	}
}

func (s *streamTableTestSuite) TestDoGet_NotProvideGetStreamFlightInfoCommandShouldReturnError() {
	testMocks := newTestMocks(s.T())
	err := testMocks.streamTable.DoGet(context.Background(), nil, testMocks.tableWriter)
	assert.True(s.T(), errors.Is(err, internalerrors.ErrInvalidArgument))
}

func (s *streamTableTestSuite) TestDoGet() {
	testCases := map[string]struct {
		testMocks              testMocks
		requestBlockTag        uint32
		requestStartSequence   int64
		requestEndSequence     int64
		requestEventsPerRecord int
		getChainEventError     error
		getBlockWithTagError   error
		expectedOutputError    error
	}{
		"request start sequence larger than request end sequence returns invalid argument error": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 2,
			requestEndSequence:   1,
			expectedOutputError:  internalerrors.ErrInvalidArgument,
		},

		"5 events with 1 mini batch": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 1,
			requestEndSequence:   6,
		},

		"10 events with 2 mini batches": {
			testMocks:            newTestMocks(s.T()),
			requestStartSequence: 1,
			requestEndSequence:   11,
		},

		"9 events with 2 mini batches and 5 events per record": {
			testMocks:              newTestMocks(s.T()),
			requestStartSequence:   1,
			requestEndSequence:     10,
			requestEventsPerRecord: 5,
		},

		"failed to getChainEvent should return internal error": {
			testMocks:              newTestMocks(s.T()),
			requestStartSequence:   1,
			requestEndSequence:     10,
			requestEventsPerRecord: 5,
			getChainEventError:     failedToGetChainEventError,
			expectedOutputError:    failedToGetChainEventError,
		},

		"failed to getBlockWithTag should return internal error": {
			testMocks:              newTestMocks(s.T()),
			requestStartSequence:   1,
			requestEndSequence:     10,
			requestEventsPerRecord: 5,
			getBlockWithTagError:   failedToGetBlockWithTagError,
			expectedOutputError:    failedToGetBlockWithTagError,
		},
	}

	for testName, tc := range testCases {
		testName := testName
		tc := tc
		s.T().Run(testName, func(t *testing.T) {
			eventsPerRecord := DefaultEventsPerRecord
			if tc.requestEventsPerRecord > 0 {
				eventsPerRecord = tc.requestEventsPerRecord
			}

			events, rawBlocks, nativeBlocks, err := getEventsAndBlocks(tc.requestBlockTag, tc.requestStartSequence, tc.requestEndSequence)
			s.Require().Nil(err)
			miniBatches := getMiniBatchInfo(tc.requestStartSequence, tc.requestEndSequence, eventsPerRecord)

			numOfCallsOfGetChainEvents := len(miniBatches)
			numOfCallsOfGetBlockWithTag := len(rawBlocks)
			if tc.getChainEventError != nil {
				numOfCallsOfGetChainEvents = 1
				numOfCallsOfGetBlockWithTag = 0
			}
			if tc.getBlockWithTagError != nil {
				numOfCallsOfGetBlockWithTag = 1
			}

			if tc.getBlockWithTagError != nil {
				tc.testMocks.session.EXPECT().Client().AnyTimes().Return(tc.testMocks.client)
				tc.testMocks.session.EXPECT().Parser().Times(0).Return(tc.testMocks.parser)
				tc.testMocks.tableWriter.EXPECT().RecordBuilder().Times(0).Return(tc.testMocks.recordBuilder)
				tc.testMocks.tableWriter.EXPECT().Flush().Times(0).Return(nil)
			} else {
				tc.testMocks.session.EXPECT().Client().Times(numOfCallsOfGetChainEvents + numOfCallsOfGetBlockWithTag).Return(tc.testMocks.client)
				tc.testMocks.session.EXPECT().Parser().Times(numOfCallsOfGetBlockWithTag).Return(tc.testMocks.parser)
				tc.testMocks.tableWriter.EXPECT().RecordBuilder().Times(numOfCallsOfGetBlockWithTag).Return(tc.testMocks.recordBuilder)
				tc.testMocks.tableWriter.EXPECT().Flush().Times(int(math.Floor(float64(numOfCallsOfGetBlockWithTag) / float64(eventsPerRecord)))).Return(nil)
			}

			for _, miniBatch := range miniBatches {
				miniBatchStart := miniBatch.sequenceNumber - tc.requestStartSequence
				miniBatchEnd := miniBatchStart + int64(miniBatch.miniBatchSize)

				tc.testMocks.client.EXPECT().GetChainEvents(
					gomock.Any(),
					&chainstorageapi.GetChainEventsRequest{
						SequenceNum:  miniBatch.sequenceNumber - 1,
						MaxNumEvents: miniBatch.miniBatchSize,
					}).
					Return(events[miniBatchStart:miniBatchEnd], tc.getChainEventError)

				if tc.getChainEventError != nil || tc.getBlockWithTagError != nil {
					break
				}
			}

			if tc.getChainEventError == nil {
				for i, nativeBlock := range nativeBlocks {
					if tc.getBlockWithTagError != nil {
						tc.testMocks.client.EXPECT().GetBlockWithTag(
							gomock.Any(),
							events[i].GetBlock().GetTag(),
							events[i].GetBlock().GetHeight(),
							events[i].GetBlock().GetHash()).
							AnyTimes().
							Return(rawBlocks[i], tc.getBlockWithTagError)
						continue
					}

					tc.testMocks.client.EXPECT().GetBlockWithTag(
						gomock.Any(),
						events[i].GetBlock().GetTag(),
						events[i].GetBlock().GetHeight(),
						events[i].GetBlock().GetHash()).
						Return(rawBlocks[i], tc.getBlockWithTagError)

					tc.testMocks.parser.EXPECT().ParseNativeBlock(
						gomock.Any(),
						rawBlocks[i]).
						Return(nativeBlock, nil)
				}
			}

			cmd := &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						StartSequence:   tc.requestStartSequence,
						EndSequence:     tc.requestEndSequence,
						EventsPerRecord: uint64(tc.requestEventsPerRecord),
					},
				},
			}

			err = tc.testMocks.streamTable.DoGet(context.Background(), cmd, tc.testMocks.tableWriter)
			assert.True(s.T(), errors.Is(err, tc.expectedOutputError))
		})
	}
}

func newTestMocks(t *testing.T) testMocks {
	mem := memory.DefaultAllocator
	tableSchema := newTestStreamedBlocksSchema()

	ctrl := gomock.NewController(t)
	session := csmocks.NewMockSession(ctrl)
	var tableParams CommonTableParams
	app := testapp.New(
		t,
		fx.Provide(func() chainstorage.Session {
			return session
		}),
		fx.Populate(&tableParams),
	)
	defer app.Close()

	return testMocks{
		ctrl:          ctrl,
		session:       session,
		client:        sdkmocks.NewMockClient(ctrl),
		parser:        sdkmocks.NewMockParser(ctrl),
		tableWriter:   xarrowmocks.NewMockTableWriter(ctrl),
		recordBuilder: array.NewRecordBuilder(mem, tableSchema),
		streamTable: NewStreamTable(
			&tableParams,
			NewTableAttributes(TableNameStreamedBlocks),
			tableSchema,
			testStreamedBlocksTable{},
			config.StreamTableConfig{
				Parallelism: 2,
			},
		),
	}
}

func getExpectedFlightEndpoints(requestStartSequence int64, requestEndSequence int64, requestEventsPerPartition uint64, storageStartSequence int64, storageEndSequence int64) ([]*flight.FlightEndpoint, error) {
	eventsPerPartition := defaultEventsPerPartition
	startSequence := requestStartSequence
	endSequence := requestEndSequence
	endpoints := make([]*flight.FlightEndpoint, 0)
	if requestEventsPerPartition > 0 {
		eventsPerPartition = requestEventsPerPartition
	}

	if startSequence > storageEndSequence {
		return nil, xerrors.Errorf("(startSequence=%d) must be less than (latestEventSequence=%d): %w", startSequence, storageEndSequence, internalerrors.ErrInvalidArgument)
	} else if startSequence < storageStartSequence {
		startSequence = storageStartSequence
	}

	if endSequence < startSequence+1 && endSequence != 0 {
		return nil, xerrors.Errorf("(endSequence=%d) must be greater than (startSequence=%d): %w", endSequence, startSequence, internalerrors.ErrInvalidArgument)
	} else if endSequence > storageEndSequence+1 || endSequence == 0 {
		endSequence = storageEndSequence + 1
	}

	for i := startSequence; i < endSequence; i += int64(eventsPerPartition) {
		ticket := &api.GetFlightInfoCmd{
			Query: &api.GetFlightInfoCmd_StreamQuery_{
				StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
					EventsPerPartition: requestEventsPerPartition,
				},
			},
		}
		ticket.GetStreamQuery().StartSequence = i
		ticket.GetStreamQuery().EndSequence = i + int64(eventsPerPartition)
		if ticket.GetStreamQuery().EndSequence > endSequence {
			ticket.GetStreamQuery().EndSequence = endSequence
		}

		ticketBytes, err := protoutil.MarshalJSON(ticket)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal ticket(%+v): %w", ticket, err)
		}

		endpoints = append(endpoints, &flight.FlightEndpoint{
			Ticket: &flight.Ticket{
				Ticket: ticketBytes,
			},
		})
	}

	return endpoints, nil
}

func getEventsAndBlocks(blockTag uint32, startSequence int64, endSequence int64) ([]*chainstorageapi.BlockchainEvent, []*chainstorageapi.Block, []*chainstorageapi.NativeBlock, error) {
	resultSize := int64(math.Max(float64(endSequence-startSequence), 0))
	events := make([]*chainstorageapi.BlockchainEvent, resultSize)
	rawBlocks := make([]*chainstorageapi.Block, resultSize)
	nativeBlocks := make([]*chainstorageapi.NativeBlock, resultSize)

	for i := startSequence; i < endSequence; i++ {
		hash := strconv.Itoa(int(i))
		height := uint64(i)
		events[i-startSequence] = &chainstorageapi.BlockchainEvent{
			SequenceNum: i,
			Type:        chainstorageapi.BlockchainEvent_BLOCK_ADDED,
			Block: &chainstorageapi.BlockIdentifier{
				Hash:   hash,
				Height: height,
				Tag:    blockTag,
			},
		}

		rawBlockHeader := &chainstorageapi.EthereumHeader{
			Hash:   hash,
			Number: height,
		}

		rawBlockData, err := json.Marshal(rawBlockHeader)
		if err != nil {
			return nil, nil, nil, xerrors.New("failed to parse raw block header")
		}

		rawBlocks[i-startSequence] = &chainstorageapi.Block{
			Blobdata: &chainstorageapi.Block_Ethereum{
				Ethereum: &chainstorageapi.EthereumBlobdata{
					Header: rawBlockData,
				},
			},
		}

		nativeBlocks[i-startSequence] = &chainstorageapi.NativeBlock{
			Block: &chainstorageapi.NativeBlock_Ethereum{
				Ethereum: &chainstorageapi.EthereumBlock{
					Header: &chainstorageapi.EthereumHeader{
						Hash:   hash,
						Number: height,
					},
				},
			},
		}
	}

	return events, rawBlocks, nativeBlocks, nil
}

func getMiniBatchInfo(startSequence int64, endSequence int64, eventsPerRecord int) []*miniBatchInfo {
	if startSequence > endSequence {
		return nil
	}

	numberOfMiniBatches := uint64(math.Ceil(float64(endSequence-startSequence) / float64(eventsPerRecord)))
	miniBatches := make([]*miniBatchInfo, numberOfMiniBatches)

	for i := startSequence; i < endSequence; i += int64(eventsPerRecord) {
		currentMiniBatchSize := eventsPerRecord
		if i+int64(eventsPerRecord) > endSequence {
			currentMiniBatchSize = int(endSequence - i)
		}

		miniBatches[int(i-startSequence)/eventsPerRecord] = &miniBatchInfo{
			sequenceNumber: i,
			miniBatchSize:  uint64(currentMiniBatchSize),
		}
	}

	return miniBatches
}

func newTestStreamedBlocksSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	return f.NewSchema(
		f.NewField("sequence", arrow.PrimitiveTypes.Uint64, "sequence number of the event"),
		f.NewField("event_type", arrow.PrimitiveTypes.Uint32, "event type"),
		f.NewField("block_data", testStreamedBlocksDataType(), "The block data"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
}

func testStreamedBlocksDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("hash", arrow.BinaryTypes.String, "Hash of the block"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block number"),
	)
}

func (t testStreamedBlocksTable) TransformBlock(ctx context.Context, blockAndEvent *BlockAndEvent, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error {
	nativeBlock, err := parser.ParseNativeBlock(ctx, blockAndEvent.Block)
	if err != nil {
		return xerrors.Errorf("failed to parse raw block to native block: %w", err)
	}

	ethereumBlock := nativeBlock.GetEthereum()
	if ethereumBlock == nil {
		return xerrors.New("failed to extract ethereum block from native block")
	}

	if err := testTransformStreamedBlocks(recordBuilder, ethereumBlock, blockAndEvent.BlockChainEvent, partitionBySize); err != nil {
		return xerrors.Errorf("failed to transform blocks: %w", err)
	}

	return nil
}

func testTransformStreamedBlocks(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, event *chainstorageapi.BlockchainEvent, partitionSize uint64) error {
	header := block.GetHeader()
	if header == nil {
		return xerrors.New("header is required")
	}

	xarrow.NewRecordAppender(recordBuilder).
		AppendUint64(uint64(event.GetSequenceNum())).
		AppendUint32(uint32(event.GetType())).
		AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendString(header.Hash).
				AppendUint64(header.Number)
		}).
		AppendUint64(partition.GetPartitionByNumber(header.Number, partitionSize)).
		AppendUint64(header.Number).
		Build()

	return nil
}
