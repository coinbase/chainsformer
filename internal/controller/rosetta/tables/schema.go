package tables

import (
	"github.com/apache/arrow/go/v10/arrow"

	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func newTransactionSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	return f.NewSchema(
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the transaction"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this transaction was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "Block number where this transaction was in"),
		f.NewField("block_timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("operations", arrow.ListOf(newOperationDataType()), "List of operations in this transaction"),
		f.NewField("operation_count", arrow.PrimitiveTypes.Uint64, "The number of operations in the transaction"),
		f.NewField("related_transactions", arrow.ListOf(newRelatedTransactionDataType()), "List of related transactions"),
		f.NewField("metadata", arrow.BinaryTypes.String, "Metadata for the block"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
}

func newBlockSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	return f.NewSchema(
		f.NewField("hash", arrow.BinaryTypes.String, "Hash of the block"),
		f.NewField("parent_hash", arrow.BinaryTypes.String, "Hash of the parent block"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block number"),
		f.NewField("parent_number", arrow.PrimitiveTypes.Uint64, "Block number of the parent block"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("transaction_count", arrow.PrimitiveTypes.Uint64, "The number of transactions in the block"),
		f.NewField("transactions", arrow.ListOf(arrow.BinaryTypes.String), "The list of transaction hashes"),
		f.NewField("metadata", arrow.BinaryTypes.String, "The number of transactions in the block"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
}

func newRawTransactionSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	transaction := f.NewSchema(
		f.NewField("block_info", newBlockDataType(), "The block containing the transaction"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "The index of the transaction within the block"),
		f.NewField("transaction_data", arrow.BinaryTypes.Binary, "The rosetta transaction content in protobuf format"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return transaction
}

func newRawStreamedTransactionSchema() *arrow.Schema {
	transactionSchema := newRawTransactionSchema()
	f := xarrow.NewSchemaFactory()

	metadataFields := []arrow.Field{
		f.NewField("_sequence_number", arrow.PrimitiveTypes.Int64, "Monotonically increasing event sequence number"),
		f.NewField("_event_type", arrow.BinaryTypes.String, "Event type UNKNOWN, BLOCK_ADDED, BLOCK_REMOVED"),
	}

	return f.NewSchema(
		append(metadataFields, transactionSchema.Fields()...)...,
	)
}

func newBlockDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("block_identifier", newBlockIdentifierType(), "the block identifier"),
		f.NewField("parent_identifier", newBlockIdentifierType(), "the parent block identifier"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
	)
}

func newBlockIdentifierType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("index", arrow.PrimitiveTypes.Uint64, "The block index"),
		f.NewField("hash", arrow.BinaryTypes.String, "The block hash"),
	)
}

func newOperationDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("operation_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the operation"),
		f.NewField("network_index", arrow.PrimitiveTypes.Uint64, "Zero-based network index of the operation"),
		f.NewField("related_operations", arrow.ListOf(newRelatedOperationDataType()), "The list of related operations"),
		f.NewField("type", arrow.BinaryTypes.String, "The operation type"),
		f.NewField("status", arrow.BinaryTypes.String, "The operation status"),
		f.NewField("account_address", arrow.BinaryTypes.String, "The address of the account"),
		f.NewField("sub_account_address", arrow.BinaryTypes.String, "The identifier of the sub account"),
		f.NewField("amount_value", xarrow.DecimalTypes.Decimal128, "The value of the transaction as an arbitrary-sized signed integer; amount_value is set to null for overflow and invalid values)"),
		f.NewField("amount_string", arrow.BinaryTypes.String, "The value of the transaction as string"),
		f.NewField("amount_symbol", arrow.BinaryTypes.String, "Canonical symbol associated with a currency"),
		f.NewField("amount_decimals", arrow.PrimitiveTypes.Uint64, "Number of decimal places in the standard unit representation of the amount"),
		f.NewField("coin_change_identifier", arrow.BinaryTypes.String, "The globally unique identifier of a coin"),
		f.NewField("coin_change_action", arrow.BinaryTypes.String, "Different state changes a coin can undergo. One of COIN_ACTION_UNSPECIFIED, COIN_CREATED, COIN_SPENT"),
		f.NewField("metadata", arrow.BinaryTypes.String, "Protocol specific information regarding the operation"),
	)
}

func newRelatedOperationDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()

	return f.NewStruct(
		f.NewField("operation_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the operation"),
		f.NewField("network_index", arrow.PrimitiveTypes.Uint64, "Zero-based network index of the operation"),
	)
}

func newRelatedTransactionDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()

	return f.NewStruct(
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction"),
		f.NewField("direction", arrow.BinaryTypes.String, "Direction of the related transaction. One of DIRECTION_UNSPECIFIED, FORWARD, BACKWARD"),
	)
}
