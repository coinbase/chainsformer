package internal

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/uber-go/tally"
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	"github.com/coinbase/chainsformer/internal/controller/internal/constant"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
	"github.com/coinbase/chainsformer/internal/utils/instrument"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

const (
	DefaultBlocksPerRecord    = uint64(1)
	DefaultEventsPerRecord    = 1
	defaultBlocksPerPartition = uint64(100)
	defaultEventsPerPartition = uint64(100)
	maxNumOfEndpoints         = 50000
)

type (
	CommonTableParams struct {
		fx.In
		fxparams.Params
		Session chainstorage.Session
	}

	Table interface {
		GetTableName() string
		GetFormat() constant.TableFormat
		GetSchema() *arrow.Schema
		GetEndpoints(ctx context.Context, cmd *api.GetFlightInfoCmd) ([]*flight.FlightEndpoint, error)
		DoGet(ctx context.Context, cmd *api.GetFlightInfoCmd, tableWriter xarrow.TableWriter) error
	}

	TableAttributesOption func(*TableAttributes)

	TableAttributes struct {
		TableName   string
		TableFormat constant.TableFormat
		Encoding    constant.Encoding
	}

	baseTable struct {
		schema                 *arrow.Schema
		tableAttributes        *TableAttributes
		instrumentGetEndpoints instrument.Call
		instrumentDoGet        instrument.Call
		counterBlocksProcessed tally.Counter
	}
)

func newBaseTable(commonParams *CommonTableParams, attributes *TableAttributes, schema *arrow.Schema) *baseTable {
	tags := map[string]string{
		"table_name":   attributes.TableName,
		"table_format": attributes.TableFormat.String(),
		"encoding":     attributes.Encoding.String(),
	}
	scope := commonParams.Metrics.SubScope("table").Tagged(tags)
	return &baseTable{
		schema:                 schema,
		tableAttributes:        attributes,
		instrumentGetEndpoints: instrument.NewCall(scope, "get_endpoints"),
		instrumentDoGet:        instrument.NewCall(scope, "do_get"),
		counterBlocksProcessed: scope.Counter("blocks_processed"),
	}
}

func (t *baseTable) GetTableName() string {
	return fmt.Sprintf("table=%v/format=%v/encoding=%v", t.tableAttributes.TableName, t.tableAttributes.TableFormat, t.tableAttributes.Encoding)
}

func (t *baseTable) GetFormat() constant.TableFormat {
	return t.tableAttributes.TableFormat
}

func (t *baseTable) GetSchema() *arrow.Schema {
	return t.schema
}

func NewTableAttributes(tableName string, opts ...TableAttributesOption) *TableAttributes {
	attributes := &TableAttributes{
		TableName:   tableName,
		TableFormat: constant.TableFormatNative,
		Encoding:    constant.EncodingNone,
	}

	for _, opt := range opts {
		opt(attributes)
	}

	return attributes
}

func WithFormat(format constant.TableFormat) TableAttributesOption {
	return func(t *TableAttributes) {
		t.TableFormat = format
	}
}

func WithEncoding(encoding constant.Encoding) TableAttributesOption {
	return func(t *TableAttributes) {
		t.Encoding = encoding
	}
}
