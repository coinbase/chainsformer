package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

type (
	connectionInfo struct {
		URL     string
		Options []grpc.DialOption
	}
)

const (
	sendMsgSize = 1024 * 1024       // 1 MB
	recvMsgSize = 1024 * 1024 * 100 // 100 MB
)

var (
	env                = flag.String("env", "local", "one of local, dev, or prod")
	blockchain         = flag.String("blockchain", "ethereum", "blockchain name")
	network            = flag.String("network", "mainnet", "network name")
	start              = flag.Uint64("start", 0, "start height")
	end                = flag.Uint64("end", 0, "end height")
	blocksPerPartition = flag.Uint64("blocks_per_partition", 100, "number of blocks per partition")
	blocksPerRecord    = flag.Uint64("blocks_per_record", 10, "number of blocks per record")
	table              = flag.String("table", "", "table name")

	logger *zap.Logger
)

func init() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	connectionInfos := map[string]*connectionInfo{
		"local": {
			URL:     "localhost:9090",
			Options: []grpc.DialOption{grpc.WithInsecure()},
		},
		"dev": {
			URL:     "localhost:9090",
			Options: []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))},
		},
		"prod": {
			URL:     "localhost:9090",
			Options: []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))},
		},
	}

	conn := connectionInfos[*env]
	if conn == nil {
		logger.Fatal("invalid env", zap.String("env", *env))
	}

	logger.Info("connection info", zap.Reflect("conn", conn))

	connOpts := append(
		conn.Options,
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(sendMsgSize), grpc.MaxCallRecvMsgSize(recvMsgSize)),
	)

	client, err := flight.NewClientWithMiddleware(conn.URL, &ClientAuth{}, []flight.ClientMiddleware{}, connOpts...)
	if err != nil {
		panic(err)
	}
	defer func(client flight.Client) {
		err := client.Close()
		if err != nil {
			logger.Error("failed to close flight client", zap.Error(err))
		}
	}(client)

	mem := memory.DefaultAllocator

	// test list flights
	ctx := context.Background()
	flightStream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		logger.Fatal("failed to light flight", zap.Error(err))
	}

	for {
		info, err := flightStream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			logger.Fatal("failed to receive flight stream", zap.Error(err))
		}

		logger.Info(fmt.Sprintf("============Flight:%s===========", info.GetFlightDescriptor().GetPath()[0]))
		logger.Info(fmt.Sprintf("%+v", info))
	}

	// test GetSchema
	logger.Info("====GetSchema===")
	getSchemaDesc, err := newFlightDescriptor(&api.GetSchemaCmd{
		Table: *table,
	})
	if err != nil {
		logger.Fatal("failed to marshal GetSchemaCmd", zap.Error(err))
	}
	res, err := client.GetSchema(ctx, getSchemaDesc)
	if err != nil {
		logger.Fatal("failed to get schema", zap.Error(err))
	}
	schema, err := flight.DeserializeSchema(res.GetSchema(), mem)
	if err != nil {
		logger.Fatal("failed to deserialize schema", zap.Error(err))
	}
	logger.Info("decoded schema", zap.Any("schema", schema))

	logger.Info("====GetFlightInfo===")
	var getFlightInfoCmd *api.GetFlightInfoCmd
	if strings.HasPrefix(*table, "streamed") {
		getFlightInfoCmd = &api.GetFlightInfoCmd{
			Query: &api.GetFlightInfoCmd_StreamQuery_{
				StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
					StartSequence:      int64(*start),
					EndSequence:        int64(*end),
					EventsPerPartition: *blocksPerPartition,
					EventsPerRecord:    *blocksPerRecord,
					Table:              *table,
				},
			},
		}
	} else {
		getFlightInfoCmd = &api.GetFlightInfoCmd{
			Query: &api.GetFlightInfoCmd_BatchQuery_{
				BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
					StartHeight:        *start,
					EndHeight:          *end,
					BlocksPerPartition: *blocksPerPartition,
					BlocksPerRecord:    *blocksPerRecord,
					Table:              *table,
				},
			},
		}
	}

	getFlightInfoDesc, err := newFlightDescriptor(getFlightInfoCmd)
	if err != nil {
		logger.Fatal("failed to marshal GetFlightInfoCmd", zap.Error(err))
	}

	receivedFlightInfo, err := client.GetFlightInfo(ctx, getFlightInfoDesc)
	if err != nil {
		logger.Fatal("failed to get flight info", zap.Error(err))
	}
	schema, err = flight.DeserializeSchema(receivedFlightInfo.Schema, mem)
	if err != nil {
		logger.Fatal("failed to deserialize schema", zap.Error(err))
	}
	logger.Info("====GetFlightInfo(schema)===")
	logger.Info("decoded schema", zap.Any("schema", schema))
	logger.Info("====GetFlightInfo(endpoints)===")
	for _, endpoint := range receivedFlightInfo.Endpoint {
		logger.Info(endpoint.String())
	}

	if len(receivedFlightInfo.Endpoint) == 0 {
		return
	}

	logger.Info("====DoAction TIP===")
	_, err = client.DoAction(ctx, &flight.Action{Type: "TIP"})
	if err != nil {
		logger.Fatal("failed to call action TIP", zap.Error(err))
	} else {
		logger.Info("success")
	}

	// test DoGet
	totalRows := int64(0)
	for i := 0; i < len(receivedFlightInfo.Endpoint); i++ {
		endpoint := receivedFlightInfo.Endpoint[i]
		logger.Info(
			"calling endpoint",
			zap.Int("i", i),
			zap.String("ticket", string(endpoint.Ticket.GetTicket())),
		)
		fData, err := client.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			logger.Fatal("failed to call do get", zap.Error(err))
		}

		r, err := flight.NewRecordReader(fData)
		if err != nil {
			logger.Fatal("failed to create record reader", zap.Error(err))
		}
		logger.Info("====DoGet===")
		for {
			rec, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}
			totalRows += rec.NumRows()
			logger.Info(fmt.Sprintf("%+v", rec))
		}
	}

	logger.Info(fmt.Sprintf("total records: %d", totalRows))
}

func newFlightDescriptor(cmd proto.Message) (*flight.FlightDescriptor, error) {
	data, err := protoutil.MarshalJSON(cmd)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal cmd as json: %w", err)
	}

	logger.Info("flight cmd", zap.String("json", string(data)))
	return &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  data,
	}, nil
}
