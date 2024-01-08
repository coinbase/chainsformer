package internal

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csmocks "github.com/coinbase/chainsformer/internal/chainstorage/mocks"
	controllermocks "github.com/coinbase/chainsformer/internal/controller/mocks"
	"github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

type (
	handlerTestSuite struct {
		suite.Suite
		ctrl              *gomock.Controller
		handler           *handler
		csSession         *csmocks.MockSession
		tables            []*controllermocks.MockTable
		serializedSchemas map[string][]byte
	}
)

const (
	testSchemaFieldName0 = "field0"
	testSchemaFieldName1 = "field1"
	testSchemaFieldName2 = "field2"

	schema0Name = "schema0"
	schema1Name = "schema1"
	schema2Name = "schema2"
)

// TODO figure out how to mock FlightService_DoGetServer and add more tests.
func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerTestSuite))
}

func (s *handlerTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *handlerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.csSession = csmocks.NewMockSession(s.ctrl)
	s.serializedSchemas = make(map[string][]byte)

	s.tables = []*controllermocks.MockTable{
		controllermocks.NewMockTable(s.ctrl), // Table with native format
		controllermocks.NewMockTable(s.ctrl), // Table with rosetta format
		controllermocks.NewMockTable(s.ctrl), // Table with rosetta format raw encoding
	}

	s.tables[0].EXPECT().GetTableName().AnyTimes().Return("table=table0/format=native/encoding=none")
	s.tables[1].EXPECT().GetTableName().AnyTimes().Return("table=table1/format=rosetta/encoding=none")
	s.tables[2].EXPECT().GetTableName().AnyTimes().Return("table=table2/format=rosetta/encoding=raw")

	f := xarrow.NewSchemaFactory()
	schema0 := f.NewSchema(f.NewField(testSchemaFieldName0, arrow.BinaryTypes.String, "test field"))
	schema1 := f.NewSchema(f.NewField(testSchemaFieldName1, arrow.BinaryTypes.String, "test field"))
	schema2 := f.NewSchema(f.NewField(testSchemaFieldName2, arrow.BinaryTypes.String, "test field"))

	s.tables[0].EXPECT().GetSchema().AnyTimes().Return(schema0)
	s.tables[1].EXPECT().GetSchema().AnyTimes().Return(schema1)
	s.tables[2].EXPECT().GetSchema().AnyTimes().Return(schema2)

	s.serializedSchemas[schema0Name] = flight.SerializeSchema(schema0, memory.DefaultAllocator)
	s.serializedSchemas[schema1Name] = flight.SerializeSchema(schema1, memory.DefaultAllocator)
	s.serializedSchemas[schema2Name] = flight.SerializeSchema(schema2, memory.DefaultAllocator)

	tableByName := make(map[string]Table, len(s.tables))
	serializedSchemas := make(map[string][]byte, len(s.tables))
	for _, table := range s.tables {
		tableName := table.GetTableName()
		_, ok := tableByName[tableName]
		s.Require().False(ok)

		tableByName[tableName] = table
		schema := table.GetSchema()
		serializedSchemas[tableName] = flight.SerializeSchema(schema, memory.DefaultAllocator)
	}

	s.handler = &handler{
		tables:            tableByName,
		SerializedSchemas: serializedSchemas,
		logger:            zaptest.NewLogger(s.T()),
		csSession:         s.csSession,
	}
}

func (s *handlerTestSuite) TestGetSchema() {
	testCases := map[string]struct {
		descriptor               *flight.FlightDescriptor
		inputCmd                 *api.GetSchemaCmd
		expectedSerializedSchema []byte
		expectedError            error
	}{
		"table 0 returns expected schema": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetSchemaCmd{
				Table: "table0",
			},
			expectedSerializedSchema: s.serializedSchemas[schema0Name],
		},

		"table 1 returns expected schema": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetSchemaCmd{
				Table:  "table1",
				Format: "rosetta",
			},
			expectedSerializedSchema: s.serializedSchemas[schema1Name],
		},

		"table 2 returns expected schema": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetSchemaCmd{
				Table:    "table2",
				Format:   "rosetta",
				Encoding: "raw",
			},
			expectedSerializedSchema: s.serializedSchemas[schema2Name],
		},

		"unable to find table returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetSchemaCmd{
				Table:  "table0",
				Format: "rosetta",
			},
			expectedError: errors.ErrNotFound,
		},

		"bad command returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorUNKNOWN,
			},
			inputCmd: &api.GetSchemaCmd{
				Table: "table0",
			},
			expectedError: errors.ErrInvalidArgument,
		},
	}

	for testName, tc := range testCases {
		tc := tc
		s.T().Run(testName, func(t *testing.T) {
			cmdData, err := protoutil.MarshalJSON(tc.inputCmd)
			s.Require().NoError(err)

			tc.descriptor.Cmd = cmdData
			schemaResult, err := s.handler.GetSchema(context.Background(), tc.descriptor)

			if tc.expectedError == nil {
				s.Require().Equal(tc.expectedSerializedSchema, schemaResult.Schema)
			} else {
				s.Require().Equal(status.Code(tc.expectedError), status.Code(err))
			}
		})
	}
}

func (s *handlerTestSuite) TestGetFlightInfo() {
	testCases := map[string]struct {
		descriptor                *flight.FlightDescriptor
		inputCmd                  *api.GetFlightInfoCmd
		expectedTable             *controllermocks.MockTable
		expectedEndpoints         []*flight.FlightEndpoint
		expectedGetEndpointsError error
		expectedSerializedSchema  []byte
		expectedError             error
	}{
		"batch: table0 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table: "table0",
					},
				},
			},
			expectedTable:            s.tables[0],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema0Name],
		},

		"batch: table1 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table:  "table1",
						Format: "rosetta",
					},
				},
			},
			expectedTable:            s.tables[1],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema1Name],
		},

		"batch: table2 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table:    "table2",
						Format:   "rosetta",
						Encoding: "raw",
					},
				},
			},
			expectedTable:            s.tables[2],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema2Name],
		},

		"stream: table0 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table: "table0",
					},
				},
			},
			expectedTable:            s.tables[0],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema0Name],
		},

		"stream: table1 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table:  "table1",
						Format: "rosetta",
					},
				},
			},
			expectedTable:            s.tables[1],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema1Name],
		},

		"stream: table2 returns expected endpoints": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table:    "table2",
						Format:   "rosetta",
						Encoding: "raw",
					},
				},
			},
			expectedTable:            s.tables[2],
			expectedEndpoints:        []*flight.FlightEndpoint{{}},
			expectedSerializedSchema: s.serializedSchemas[schema2Name],
		},

		"batch: table0 unable to find table returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table:  "table0",
						Format: "rosetta",
					},
				},
			},
			expectedEndpoints: []*flight.FlightEndpoint{{}},
			expectedError:     errors.ErrNotFound,
		},

		"batch: table0 bad command type returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorUNKNOWN,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table: "table0",
					},
				},
			},
			expectedEndpoints: []*flight.FlightEndpoint{{}},
			expectedError:     errors.ErrInvalidArgument,
		},

		"batch: table1 failed to get endpoints returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table:  "table1",
						Format: "rosetta",
					},
				},
			},
			expectedTable:             s.tables[1],
			expectedGetEndpointsError: status.Errorf(codes.Internal, ""),
			expectedError:             status.Errorf(codes.Internal, ""),
		},

		"stream: table1 unable to find table returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table: "table1",
					},
				},
			},
			expectedEndpoints: []*flight.FlightEndpoint{{}},
			expectedError:     errors.ErrNotFound,
		},

		"batch: table2 failed to get endpoints returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_BatchQuery_{
					BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
						Table:    "table2",
						Format:   "rosetta",
						Encoding: "raw",
					},
				},
			},
			expectedTable:             s.tables[2],
			expectedGetEndpointsError: status.Errorf(codes.Internal, ""),
			expectedError:             status.Errorf(codes.Internal, ""),
		},

		"stream: table2 unable to find table returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table: "table2",
					},
				},
			},
			expectedEndpoints: []*flight.FlightEndpoint{{}},
			expectedError:     errors.ErrNotFound,
		},

		"stream: table 0 bad command type returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorUNKNOWN,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table: "table0",
					},
				},
			},
			expectedEndpoints: []*flight.FlightEndpoint{{}},
			expectedError:     errors.ErrInvalidArgument,
		},

		"stream: table 1 failed to get endpoints returns error": {
			descriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
			},
			inputCmd: &api.GetFlightInfoCmd{
				Query: &api.GetFlightInfoCmd_StreamQuery_{
					StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
						Table:  "table1",
						Format: "rosetta",
					},
				},
			},
			expectedTable:             s.tables[1],
			expectedGetEndpointsError: status.Errorf(codes.Internal, ""),
			expectedError:             status.Errorf(codes.Internal, ""),
		},
	}

	for testName, tc := range testCases {
		tc := tc
		s.T().Run(testName, func(t *testing.T) {
			cmdData, err := protoutil.MarshalJSON(tc.inputCmd)
			s.Require().NoError(err)

			tc.descriptor.Cmd = cmdData

			if tc.expectedTable != nil {
				tc.expectedTable.EXPECT().GetEndpoints(gomock.Any(), tc.inputCmd).Times(1).Return(tc.expectedEndpoints, tc.expectedGetEndpointsError)
			}

			schemaResult, err := s.handler.GetFlightInfo(context.Background(), tc.descriptor)

			if tc.expectedError == nil {
				s.Require().Equal(tc.expectedSerializedSchema, schemaResult.Schema)
			} else {
				s.Require().Equal(status.Code(tc.expectedError), status.Code(err))
			}
		})
	}
}
