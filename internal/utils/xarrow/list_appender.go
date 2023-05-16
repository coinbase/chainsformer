package xarrow

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
)

type (
	ListAppender struct {
		listBuilder *array.ListBuilder
		index       int
	}

	ListBuilderFn func(listBuilder *array.ListBuilder)
)

func NewListAppender(listBuilder *array.ListBuilder) *ListAppender {
	return &ListAppender{
		listBuilder: listBuilder,
		index:       0,
	}
}

func (a *ListAppender) AppendString(value string) *ListAppender {
	a.next().(*array.StringBuilder).Append(value)
	return a
}

func (a *ListAppender) AppendUint32(value uint32) *ListAppender {
	a.next().(*array.Uint32Builder).Append(value)
	return a
}

func (a *ListAppender) AppendUint64(value uint64) *ListAppender {
	a.next().(*array.Uint64Builder).Append(value)
	return a
}

func (a *ListAppender) AppendFloat64(value float64) *ListAppender {
	a.next().(*array.Float64Builder).Append(value)
	return a
}

func (a *ListAppender) AppendBool(value bool) *ListAppender {
	a.next().(*array.BooleanBuilder).Append(value)
	return a
}

func (a *ListAppender) AppendDecimal128(value decimal128.Num) *ListAppender {
	a.next().(*array.Decimal128Builder).Append(value)
	return a
}

func (a *ListAppender) AppendStruct(cb func(sa *StructAppender)) *ListAppender {
	sa := NewStructAppender(a.next().(*array.StructBuilder))
	cb(sa)
	sa.build()
	return a
}

func (a *ListAppender) AppendList(cb func(la *ListAppender)) *ListAppender {
	la := NewListAppender(a.next().(*array.ListBuilder))
	cb(la)
	la.build()
	return a
}

func (a *ListAppender) next() array.Builder {
	if a.index == 0 {
		a.listBuilder.Append(true)
	}
	builder := a.listBuilder.ValueBuilder()
	a.index += 1
	return builder
}

func (a *ListAppender) build() {
	if a.index == 0 {
		a.listBuilder.AppendNull()
	}

	a.index = 0
}
