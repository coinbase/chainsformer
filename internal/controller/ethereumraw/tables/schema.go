package tables

import (
	"github.com/apache/arrow/go/v10/arrow"

	"github.com/coinbase/chainsformer/internal/config"
	ethereumTables "github.com/coinbase/chainsformer/internal/controller/ethereum/tables"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func newTransactionSchema(config *config.Config) *arrow.Schema {
	f := xarrow.NewSchemaFactory()

	transaction := f.NewSchema(
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the transaction"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this transaction was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "Block number where this transaction was in"),
		f.NewField("block_timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("from_address", arrow.BinaryTypes.String, "Address of the sender"),
		f.NewField("to_address", arrow.BinaryTypes.String, "Address of the receiver. Empty when its a contract creation transaction"),
		f.NewField("nonce", arrow.PrimitiveTypes.Uint64, "The number of transactions made by the sender prior to this one"),
		f.NewField("value_string", arrow.BinaryTypes.String, "Value transferred in Wei as string"),
		f.NewField("gas", arrow.PrimitiveTypes.Uint64, "Gas provided by the sender"),
		f.NewField("gas_price", arrow.PrimitiveTypes.Uint64, "Gas price provided by the sender in Wei"),
		f.NewField("input", arrow.BinaryTypes.String, "The data sent along with the transaction"),
		f.NewField("transaction_type", arrow.PrimitiveTypes.Uint64, "Transaction type. One of 0 (Legacy), 1 (Legacy), 2 (EIP-1559)"),
		f.NewField("max_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Total fee that covers both base and priority fees"),
		f.NewField("max_priority_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Fee given to miners to incentivize them to include the transaction"),
		f.NewField("priority_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Fee given to miners to incentivize them to include the transaction"),
		f.NewField("block", ethereumTables.NewBlockDataType(), "The block containing this transaction"),
		f.NewField("receipt", ethereumTables.NewReceiptDataType(config), "The transaction receipt"),
		f.NewField("traces", arrow.ListOf(newTraceDataType()), "The list of transaction traces"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return transaction
}

func newTraceDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Transaction hash where this trace was in"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Transaction index where this trace was in"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this trace was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "Block number where this trace was in"),
		f.NewField("from_address", arrow.BinaryTypes.String, "Address of the sender, empty when trace_type is genesis or reward"),
		f.NewField("to_address", arrow.BinaryTypes.String, "Address of the receiver if trace_type is call, address of new contract or null if trace_type is create, beneficiary address if trace_type is suicide, miner address if trace_type is reward, shareholder address if trace_type is genesis, WithdrawDAO address if trace_type is daofork"),
		f.NewField("value_string", arrow.BinaryTypes.String, "Value transferred in Wei as string"),
		f.NewField("input", arrow.BinaryTypes.String, "The data sent along with the message call"),
		f.NewField("output", arrow.BinaryTypes.String, "The output of the message call, bytecode of contract when trace_type is create"),
		f.NewField("type", arrow.BinaryTypes.String, "Trace type"),
		f.NewField("trace_type", arrow.BinaryTypes.String, "One of call, create, suicide, reward, genesis, daofork"),
		f.NewField("call_type", arrow.BinaryTypes.String, "One of call, callcode, delegatecall, staticcall"),
		f.NewField("gas", arrow.PrimitiveTypes.Uint64, "Gas provided with the message call"),
		f.NewField("gas_used", arrow.PrimitiveTypes.Uint64, "Gas used by the message call"),
		f.NewField("subtraces", arrow.PrimitiveTypes.Uint64, "Number of subtraces"),
		f.NewField("trace_address", arrow.ListOf(arrow.PrimitiveTypes.Uint64), "The list of trace address in call tree"),
		f.NewField("error", arrow.BinaryTypes.String, "Error if message call failed"),
		f.NewField("status", arrow.PrimitiveTypes.Uint64, "Either 1 (success) or 0 (failure, due to any operation that can cause the call itself or any top-level call to revert)"),
		f.NewField("trace_id", arrow.BinaryTypes.String, "Unique string that identifies the trace. For transaction-scoped traces it is {trace_type}_{transaction_hash}_{trace_address}. For block-scoped traces it is {trace_type}_{block_number}_{index_within_block}"),
	)
}

func newStreamedTransactionSchema(config *config.Config) *arrow.Schema {
	transactionSchema := newTransactionSchema(config)
	f := xarrow.NewSchemaFactory()

	metadataFields := []arrow.Field{
		f.NewField("_sequence_number", arrow.PrimitiveTypes.Int64, "Monotonically increasing event sequence number"),
		f.NewField("_event_type", arrow.BinaryTypes.String, "Event type UNKNOWN, BLOCK_ADDED, BLOCK_REMOVED"),
	}

	return f.NewSchema(
		append(metadataFields, transactionSchema.Fields()...)...,
	)
}
