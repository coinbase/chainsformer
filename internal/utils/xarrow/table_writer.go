package xarrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

type (
	TableWriter interface {
		RecordBuilder() *array.RecordBuilder
		Flush() error
		Close() error
	}

	tableWriterImpl struct {
		mem           memory.Allocator
		logger        *zap.Logger
		writer        *flight.Writer
		recordBuilder *array.RecordBuilder
	}

	RecordBuilderFn func(recordBuilder *array.RecordBuilder) (bool, error)
)

func NewTableWriter(logger *zap.Logger, tableSchema *arrow.Schema, fwriter flight.DataStreamWriter) (TableWriter, error) {
	mem := memory.DefaultAllocator
	return &tableWriterImpl{
		logger:        logger,
		mem:           mem,
		writer:        flight.NewRecordWriter(fwriter, ipc.WithSchema(tableSchema)),
		recordBuilder: array.NewRecordBuilder(mem, tableSchema),
	}, nil
}

func (t *tableWriterImpl) RecordBuilder() *array.RecordBuilder {
	return t.recordBuilder
}

func (t *tableWriterImpl) Flush() error {
	rec := t.recordBuilder.NewRecord()
	defer func() {
		rec.Release()
	}()

	t.logger.Info("writing record", zap.Int64("rows", rec.NumRows()))
	if err := t.writer.Write(rec); err != nil {
		return xerrors.Errorf("failed to write record: %w", err)
	}
	return nil
}

func (t *tableWriterImpl) Close() error {
	err := t.writer.Close()
	if err != nil {
		return xerrors.Errorf("failed to close flight record writer: %w", err)
	}
	return nil
}
