package tables

import (
	"github.com/apache/arrow/go/v10/arrow"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainsformer/internal/config"
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
		f.NewField("value", xarrow.DecimalTypes.Decimal128, "Value transferred in Wei as decimal"),
		f.NewField("value_string", arrow.BinaryTypes.String, "Value transferred in Wei as string"),
		f.NewField("gas", arrow.PrimitiveTypes.Uint64, "Gas provided by the sender"),
		f.NewField("gas_price", arrow.PrimitiveTypes.Uint64, "Gas price provided by the sender in Wei"),
		f.NewField("input", arrow.BinaryTypes.String, "The data sent along with the transaction"),
		f.NewField("transaction_type", arrow.PrimitiveTypes.Uint64, "Transaction type. One of 0 (Legacy), 1 (Legacy), 2 (EIP-1559)"),
		f.NewField("max_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Total fee that covers both base and priority fees"),
		f.NewField("max_priority_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Fee given to miners to incentivize them to include the transaction"),
		f.NewField("priority_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Fee given to miners to incentivize them to include the transaction"),
		f.NewField("block", NewBlockDataType(), "The block containing this transaction"),
		f.NewField("receipt", NewReceiptDataType(config), "The transaction receipt"),
		f.NewField("traces", arrow.ListOf(newTraceDataType()), "The list of transaction traces"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return transaction
}

func newRawTransactionSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	transaction := f.NewSchema(
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the transaction"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this transaction was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "Block number where this transaction was in"),
		f.NewField("block_timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("transaction_data", arrow.BinaryTypes.Binary, "The native transaction data in protobuf format"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return transaction
}

func newBlockSchema(config *config.Config) *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	commonFields := []arrow.Field{
		f.NewField("hash", arrow.BinaryTypes.String, "Hash of the block"),
		f.NewField("parent_hash", arrow.BinaryTypes.String, "Hash of the parent block"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block number"),
		f.NewField("nonce", arrow.BinaryTypes.String, "Hash of the generated proof-of-work"),
		f.NewField("sha3_uncles", arrow.BinaryTypes.String, "SHA3 of the uncles data in the block"),
		f.NewField("logs_bloom", arrow.BinaryTypes.String, "The bloom filter for the logs of the block"),
		f.NewField("transactions_root", arrow.BinaryTypes.String, "The root of the transaction trie of the block"),
		f.NewField("state_root", arrow.BinaryTypes.String, "The root of the final state trie of the block"),
		f.NewField("receipts_root", arrow.BinaryTypes.String, "The root of the receipts trie of the block"),
		f.NewField("miner", arrow.BinaryTypes.String, "The address of the beneficiary to whom the mining rewards were given"),
		f.NewField("difficulty", arrow.PrimitiveTypes.Uint64, "Integer of the difficulty for this block"),
		f.NewField("total_difficulty", xarrow.DecimalTypes.Decimal128, "Integer of the total difficulty of the chain until this block"),
		f.NewField("size", arrow.PrimitiveTypes.Uint64, "The size of this block in bytes"),
		f.NewField("extra_data", arrow.BinaryTypes.String, "The extra data field of this block"),
		f.NewField("gas_limit", arrow.PrimitiveTypes.Uint64, "The maximum gas allowed in this block"),
		f.NewField("gas_used", arrow.PrimitiveTypes.Uint64, "The total used gas by all transactions in this block"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("transaction_count", arrow.PrimitiveTypes.Uint64, "The number of transactions in the block"),
		f.NewField("base_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Protocol base fee per gas, which can move up or down"),
		f.NewField("transactions", arrow.ListOf(arrow.BinaryTypes.String), "The list of transaction hashes"),
		f.NewField("uncles", arrow.ListOf(arrow.BinaryTypes.String), "The list of uncle hashes"),
		f.NewField("uncle_blocks", arrow.ListOf(NewBlockDataType()), "The list of uncle blocks"),
	}

	switch config.Blockchain() {
	case common.Blockchain_BLOCKCHAIN_ETHEREUM:
		commonFields = append(
			commonFields,
			f.NewField("withdrawals", arrow.ListOf(newWithdrawalDataType()), "The list of withdrawals"),
			f.NewField("withdrawals_root", arrow.BinaryTypes.String, "The root of the withdrawals trie of the block"),
		)
	}

	commonFields = append(
		commonFields,
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return f.NewSchema(
		commonFields...,
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

func newStreamedBlocksSchema(config *config.Config) *arrow.Schema {
	blockSchema := newBlockSchema(config)
	f := xarrow.NewSchemaFactory()

	metadataFields := []arrow.Field{
		f.NewField("_sequence_number", arrow.PrimitiveTypes.Int64, "Monotonically increasing event sequence number"),
		f.NewField("_event_type", arrow.BinaryTypes.String, "Event type UNKNOWN, BLOCK_ADDED, BLOCK_REMOVED"),
	}

	return f.NewSchema(
		append(metadataFields, blockSchema.Fields()...)...,
	)
}

func NewBlockDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("hash", arrow.BinaryTypes.String, "Hash of the block"),
		f.NewField("parent_hash", arrow.BinaryTypes.String, "Hash of the parent block"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block number"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The unix timestamp for when the block was collated"),
		f.NewField("miner", arrow.BinaryTypes.String, "The address of the beneficiary to whom the mining rewards were given"),
		f.NewField("difficulty", arrow.PrimitiveTypes.Uint64, "Integer of the difficulty for this block"),
		f.NewField("gas_limit", arrow.PrimitiveTypes.Uint64, "The maximum gas allowed in this block"),
		f.NewField("gas_used", arrow.PrimitiveTypes.Uint64, "The total used gas by all transactions in this block"),
		f.NewField("base_fee_per_gas", arrow.PrimitiveTypes.Uint64, "Protocol base fee per gas, which can move up or down"),
	)
}

func NewReceiptDataType(config *config.Config) arrow.DataType {
	f := xarrow.NewSchemaFactory()
	commonFields := []arrow.Field{
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Zero-based index of the transaction"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this transaction was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "Block number where this transaction was in"),
		f.NewField("from_address", arrow.BinaryTypes.String, "Address of the sender"),
		f.NewField("to_address", arrow.BinaryTypes.String, "Address of the receiver. Empty when its a contract creation transaction"),
		f.NewField("cumulative_gas_used", arrow.PrimitiveTypes.Uint64, "The total amount of gas used when this transaction was executed in the block"),
		f.NewField("gas_used", arrow.PrimitiveTypes.Uint64, "The amount of gas used by this specific transaction alone"),
		f.NewField("contract_address", arrow.BinaryTypes.String, "The contract address created, if the transaction was a contract creation, otherwise empty"),
		f.NewField("logs", arrow.ListOf(newLogDataType()), "Array of log objects, which this transaction generated"),
		f.NewField("logs_bloom", arrow.BinaryTypes.String, "Bloom filter for light clients to quickly retrieve related logs"),
		f.NewField("root", arrow.BinaryTypes.String, "32 bytes of post-transaction stateroot (pre Byzantium)"),
		f.NewField("type", arrow.PrimitiveTypes.Uint64, "Transaction type. One of 0 (Legacy), 1 (Legacy), 2 (EIP-1559)"),
		f.NewField("status", arrow.PrimitiveTypes.Uint64, "Either 1 (success) or 0 (failure) (post Byzantium)"),
		f.NewField("effective_gas_price", arrow.PrimitiveTypes.Uint64, "The actual value per gas deducted from the senders account. Replacement of gas_price after EIP-1559"),
	}

	switch config.Blockchain() {
	case common.Blockchain_BLOCKCHAIN_ARBITRUM:
		commonFields = append(
			commonFields,
			f.NewField("l1_gas_used", arrow.PrimitiveTypes.Uint64, "The costs to send the input call data to L1"),
		)
	case common.Blockchain_BLOCKCHAIN_OPTIMISM:
		commonFields = append(
			commonFields,
			f.NewField("l1_gas_used", arrow.PrimitiveTypes.Uint64, "The costs to send the input call data to L1"),
			f.NewField("l1_gas_price", arrow.PrimitiveTypes.Uint64, "The gas price on L1"),
			f.NewField("l1_fee", arrow.PrimitiveTypes.Uint64, "The amount in wei paid on L1"),
			f.NewField("l1_fee_scalar", arrow.BinaryTypes.String, "Variable parameter that makes sure that gas costs on L1 get covered + profits"),
		)
	}

	return f.NewStruct(
		commonFields...,
	)
}

func newLogDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("log_index", arrow.PrimitiveTypes.Uint64, "Integer of the log index position in the block"),
		f.NewField("transaction_hash", arrow.BinaryTypes.String, "Hash of the transaction this log was created from"),
		f.NewField("transaction_index", arrow.PrimitiveTypes.Uint64, "Integer of the transactions index position log was created from"),
		f.NewField("block_hash", arrow.BinaryTypes.String, "Hash of the block where this log was in"),
		f.NewField("block_number", arrow.PrimitiveTypes.Uint64, "The block number where this log was in"),
		f.NewField("address", arrow.BinaryTypes.String, "Address from which this log originated"),
		f.NewField("data", arrow.BinaryTypes.String, "Contains one or more 32 Bytes non-indexed arguments of the log"),
		f.NewField("topics", arrow.ListOf(arrow.BinaryTypes.String), "Indexed log arguments (0 to 4 32-byte hex strings)"),
		f.NewField("removed", arrow.FixedWidthTypes.Boolean, "True when the log was removed, due to a chain reorganization. false if its a valid log."),
	)
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
		f.NewField("value", xarrow.DecimalTypes.Decimal128, "Value transferred in Wei as decimal"),
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

func newWithdrawalDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("index", arrow.PrimitiveTypes.Uint64, "Value that increments by 1 per withdrawal to uniquely identify each withdrawal"),
		f.NewField("validator_index", arrow.PrimitiveTypes.Uint64, "The validator index of the validator on the consensus layer"),
		f.NewField("address", arrow.BinaryTypes.String, "The recipient address for the withdrawn ether"),
		f.NewField("amount", arrow.PrimitiveTypes.Uint64, "A non zero amount of ether given in Gwei"),
	)
}
