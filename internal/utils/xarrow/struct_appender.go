package xarrow

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
)

type (
	StructAppender struct {
		structBuilder *array.StructBuilder
		index         int
	}

	StructBuilderFn func(structBuilder *array.StructBuilder, index int)
)

func NewStructAppender(structBuilder *array.StructBuilder) *StructAppender {
	return &StructAppender{
		structBuilder: structBuilder,
		index:         0,
	}
}

func (a *StructAppender) AppendString(value string) *StructAppender {
	a.next().(*array.StringBuilder).Append(value)
	return a
}

func (a *StructAppender) AppendUint32(value uint32) *StructAppender {
	a.next().(*array.Uint32Builder).Append(value)
	return a
}

func (a *StructAppender) AppendUint64(value uint64) *StructAppender {
	a.next().(*array.Uint64Builder).Append(value)
	return a
}

func (a *StructAppender) AppendFloat64(value float64) *StructAppender {
	a.next().(*array.Float64Builder).Append(value)
	return a
}

func (a *StructAppender) AppendBool(value bool) *StructAppender {
	a.next().(*array.BooleanBuilder).Append(value)
	return a
}

func (a *StructAppender) AppendDecimal128(value decimal128.Num) *StructAppender {
	a.next().(*array.Decimal128Builder).Append(value)
	return a
}

func (a *StructAppender) AppendDecimal128Null() *StructAppender {
	a.next().(*array.Decimal128Builder).AppendNull()
	return a
}

func (a *StructAppender) AppendStruct(cb func(sa *StructAppender)) *StructAppender {
	sa := NewStructAppender(a.next().(*array.StructBuilder))
	cb(sa)
	sa.build()
	return a
}

func (a *StructAppender) AppendList(cb func(la *ListAppender)) *StructAppender {
	la := NewListAppender(a.next().(*array.ListBuilder))
	cb(la)
	la.build()
	return a
}

func (a *StructAppender) next() array.Builder {
	if a.index == 0 {
		a.structBuilder.Append(true)
	}
	builder := a.structBuilder.FieldBuilder(a.index)
	a.index += 1
	return builder
}

func (a *StructAppender) build() {
	if a.index == 0 {
		a.structBuilder.AppendNull()
	}

	a.index = 0
}
