package xarrow

import (
	"github.com/apache/arrow/go/v10/arrow"
)

type (
	SchemaFactory struct{}
)

var (
	descriptionKeys = []string{"description"}
)

func NewSchemaFactory() SchemaFactory {
	return SchemaFactory{}
}

func (f SchemaFactory) NewSchema(fields ...arrow.Field) *arrow.Schema {
	return arrow.NewSchema(fields, nil)
}

func (f SchemaFactory) NewField(name string, dataType arrow.DataType, description string) arrow.Field {
	return arrow.Field{
		Name:     name,
		Type:     dataType,
		Nullable: true,
		Metadata: f.newMetadata(description),
	}
}

func (f SchemaFactory) NewStruct(fields ...arrow.Field) *arrow.StructType {
	return arrow.StructOf(fields...)
}

func (f SchemaFactory) NewList(dt arrow.DataType) *arrow.ListType {
	return arrow.ListOf(dt)
}

func (f SchemaFactory) newMetadata(description string) arrow.Metadata {
	return arrow.NewMetadata(descriptionKeys, []string{description})
}
