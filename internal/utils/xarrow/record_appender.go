package xarrow

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
)

type (
	RecordAppender struct {
		recordBuilder *array.RecordBuilder
		index         int
	}
)

func NewRecordAppender(recordBuilder *array.RecordBuilder) *RecordAppender {
	return &RecordAppender{
		recordBuilder: recordBuilder,
		index:         0,
	}
}

func (a *RecordAppender) Build() {
	a.index = 0
}

func (a *RecordAppender) AppendString(value string) *RecordAppender {
	a.next().(*array.StringBuilder).Append(value)
	return a
}

func (a *RecordAppender) AppendInt32(value int32) *RecordAppender {
	a.next().(*array.Int32Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendUint32(value uint32) *RecordAppender {
	a.next().(*array.Uint32Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendInt64(value int64) *RecordAppender {
	a.next().(*array.Int64Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendUint64(value uint64) *RecordAppender {
	a.next().(*array.Uint64Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendFloat64(value float64) *RecordAppender {
	a.next().(*array.Float64Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendBool(value bool) *RecordAppender {
	a.next().(*array.BooleanBuilder).Append(value)
	return a
}

func (a *RecordAppender) AppendDecimal128(value decimal128.Num) *RecordAppender {
	a.next().(*array.Decimal128Builder).Append(value)
	return a
}

func (a *RecordAppender) AppendStruct(cb func(sa *StructAppender)) *RecordAppender {
	sa := NewStructAppender(a.next().(*array.StructBuilder))
	cb(sa)
	sa.build()
	return a
}

func (a *RecordAppender) AppendList(cb func(la *ListAppender)) *RecordAppender {
	la := NewListAppender(a.next().(*array.ListBuilder))
	cb(la)
	la.build()
	return a
}

func (a *RecordAppender) AppendBinary(value []byte) *RecordAppender {
	a.next().(*array.BinaryBuilder).Append(value)
	return a
}

func (a *RecordAppender) next() array.Builder {
	builder := a.recordBuilder.Field(a.index)
	a.index += 1
	return builder
}
