package internal

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	"github.com/coinbase/chainsformer/internal/controller/internal/constant"
	"github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/finalizer"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
	"github.com/coinbase/chainsformer/internal/utils/log"
	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

const (
	flightActionTip            = "TIP"
	flightActionEarliest       = "EARLIEST"
	flightStreamActionTip      = "STREAM_TIP"
	flightStreamActionEarliest = "STREAM_EARLIEST"
)

type (
	Handler = flight.FlightServer

	HandlerParams struct {
		fx.In
		fxparams.Params
		CSSession  chainstorage.Session
		Controller Controller
	}

	handler struct {
		flight.BaseFlightServer
		SerializedSchemas map[string][]byte
		tables            map[string]Table
		logger            *zap.Logger
		csSession         chainstorage.Session
	}
)

func NewHandler(params HandlerParams) (Handler, error) {
	logger := log.WithPackageName(params.Logger, packageName)
	scope := params.Metrics
	controller := params.Controller
	tables := controller.Tables()
	if len(tables) == 0 {
		return nil, xerrors.Errorf("tables is empty")
	}
	tableByName := make(map[string]Table, len(tables))
	serializedSchemas := make(map[string][]byte, len(tables))
	for _, table := range tables {
		tableName := table.GetTableName()
		_, ok := tableByName[tableName]
		if ok {
			return nil, xerrors.Errorf("found duplicated table names: %s", tableName)
		}
		tableByName[tableName] = table
		schema := table.GetSchema()
		serializedSchemas[tableName] = flight.SerializeSchema(schema, memory.DefaultAllocator)
	}

	h := Handler(&handler{
		tables:            tableByName,
		SerializedSchemas: serializedSchemas,
		logger:            logger,
		csSession:         params.CSSession,
	})
	h = withErrorInterceptor(h)
	h = withInstrumentInterceptor(h, scope, logger)
	return h, nil
}

func (h *handler) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	for table, schema := range h.SerializedSchemas {
		err := fs.Send(&flight.FlightInfo{
			Schema: schema,
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{table},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		})
		if err != nil {
			return xerrors.Errorf("failed to send flight info: %w", err)
		}
	}

	return nil
}

func (h *handler) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if in.Type != flight.DescriptorCMD {
		return nil, xerrors.Errorf("invalid type(%v): %w", in.Type, errors.ErrInvalidArgument)
	}

	var cmd api.GetSchemaCmd
	if err := protoutil.UnmarshalJSON(in.Cmd, &cmd); err != nil {
		return nil, xerrors.Errorf("failed to decode cmd: %v :%w", err, errors.ErrInvalidArgument)
	}

	table := getTableNameFromGetSchemaCmd(&cmd)
	serializedSchema := h.SerializedSchemas[table]
	if serializedSchema == nil {
		return nil, xerrors.Errorf("table(%v): %w", table, errors.ErrNotFound)
	}

	return &flight.SchemaResult{Schema: serializedSchema}, nil
}

func (h *handler) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if in.Type != flight.DescriptorCMD {
		return nil, xerrors.Errorf("invalid type(%v): %w", in.Type, errors.ErrInvalidArgument)
	}

	var cmd api.GetFlightInfoCmd
	if err := protoutil.UnmarshalJSON(in.Cmd, &cmd); err != nil {
		return nil, xerrors.Errorf("failed to decode cmd: %v: %w", err, errors.ErrInvalidArgument)
	}

	tableName := getTableNameFromGetFlightInfoCmd(&cmd)
	h.logger.Info("decoded cmd", zap.Reflect("cmd", &cmd), zap.Reflect("table_name", tableName))
	table := h.tables[tableName]
	if table == nil {
		return nil, xerrors.Errorf("table(%v): %w", tableName, errors.ErrNotFound)
	}

	serializedSchema := h.SerializedSchemas[tableName]
	if serializedSchema == nil {
		return nil, xerrors.Errorf("schema for table(%v): %w", tableName, errors.ErrNotFound)
	}

	endpoints, err := table.GetEndpoints(ctx, &cmd)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoints for table(%v): %w", tableName, err)
	}

	return &flight.FlightInfo{
		Schema:           serializedSchema,
		FlightDescriptor: in,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (h *handler) DoAction(action *flight.Action, fs flight.FlightService_DoActionServer) error {
	switch t := action.Type; t {
	case flightActionTip:
		endHeight, err := h.csSession.GetTipHeight(fs.Context())
		if err != nil {
			return xerrors.Errorf("failed to get chain tip height: %w", err)
		}

		err = fs.Send(
			&flight.Result{
				Body: []byte(strconv.FormatUint(endHeight, 10)),
			})
		if err != nil {
			return xerrors.Errorf("failed to send chain tip height: %w", err)
		}

		return nil
	case flightActionEarliest:
		startHeight, err := h.csSession.GetStartHeight(fs.Context())
		if err != nil {
			return xerrors.Errorf("failed to get chain start height: %w", err)
		}

		err = fs.Send(
			&flight.Result{
				Body: []byte(strconv.FormatUint(startHeight, 10)),
			})
		if err != nil {
			return xerrors.Errorf("failed to send chain tip height: %w", err)
		}

		return nil
	case flightStreamActionEarliest:
		fallthrough
	case flightStreamActionTip:
		position := chainstorage.LatestEventPosition
		if t == flightStreamActionEarliest {
			position = chainstorage.EarliestEventPosition
		}

		seq, err := h.csSession.GetEventSequenceByPosition(fs.Context(), position)
		if err != nil {
			return xerrors.Errorf("failed to get event sequence: %w", err)
		}

		err = fs.Send(
			&flight.Result{
				Body: []byte(strconv.FormatInt(seq, 10)),
			})
		if err != nil {
			return xerrors.Errorf("failed to send event sequence: %w", err)
		}

		return nil
	default:
		return xerrors.Errorf("unsupported actionType(%v): %w", t, errors.ErrInvalidArgument)
	}
}

func (h *handler) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	var cmd api.GetFlightInfoCmd
	if tkt.Ticket != nil && len(tkt.Ticket) > 0 {
		err := protoutil.UnmarshalJSON(tkt.Ticket, &cmd)
		if err != nil {
			return xerrors.Errorf("failed to decode ticket: %v: %w", err, errors.ErrInvalidArgument)
		}
	}

	tableName := getTableNameFromGetFlightInfoCmd(&cmd)
	h.logger.Info("decoded ticket cmd", zap.Reflect("cmd", &cmd), zap.Reflect("table_name", tableName))
	table := h.tables[tableName]
	if table == nil {
		return xerrors.Errorf("table(%v): %w", tableName, errors.ErrNotFound)
	}
	tableSchema := table.GetSchema()

	tableWriter, err := xarrow.NewTableWriter(h.logger, tableSchema, fs)
	if err != nil {
		return xerrors.Errorf("failed to create table writer for table(%s): %w", tableName, err)
	}

	finalizer := finalizer.WithCloser(tableWriter)
	defer finalizer.Finalize()

	if err := table.DoGet(fs.Context(), &cmd, tableWriter); err != nil {
		return xerrors.Errorf("failed to execute DoGet on table(=%s): %w", tableName, err)
	}

	if err := tableWriter.Flush(); err != nil {
		return xerrors.Errorf("failed to flush table writer for table(=%s): %w", tableName, err)
	}

	return finalizer.Close()
}

func getTableNameFromGetFlightInfoCmd(cmd *api.GetFlightInfoCmd) string {
	tableName := ""
	tableFormat := ""
	encoding := ""

	if cmd.GetBatchQuery() != nil {
		tableName = cmd.GetBatchQuery().GetTable()
		tableFormat = cmd.GetBatchQuery().GetFormat()
		encoding = cmd.GetBatchQuery().GetEncoding()
	}
	if cmd.GetStreamQuery() != nil {
		tableName = cmd.GetStreamQuery().GetTable()
		tableFormat = cmd.GetStreamQuery().GetFormat()
		encoding = cmd.GetStreamQuery().GetEncoding()
	}

	if tableFormat == "" {
		tableFormat = constant.TableFormatNative.String()
	}

	if encoding == "" {
		encoding = constant.EncodingNone.String()
	}

	return fmt.Sprintf("table=%v/format=%v/encoding=%v", tableName, tableFormat, encoding)
}

func getTableNameFromGetSchemaCmd(cmd *api.GetSchemaCmd) string {
	tableName := cmd.Table
	tableFormat := cmd.GetFormat()
	encoding := cmd.GetEncoding()

	if tableFormat == "" {
		tableFormat = constant.TableFormatNative.String()
	}

	if encoding == "" {
		encoding = constant.EncodingNone.String()
	}

	return fmt.Sprintf("table=%v/format=%v/encoding=%v", tableName, tableFormat, encoding)
}
