package tables

import (
	"github.com/apache/arrow/go/v10/arrow"

	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func newTransactionSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	transaction := f.NewSchema(
		f.NewField("hash", arrow.BinaryTypes.String, "The transaction hash"),
		f.NewField("size", arrow.PrimitiveTypes.Uint64, "The serialized transaction size"),
		f.NewField("virtual_size", arrow.PrimitiveTypes.Uint64, "The virtual transaction size (differs from size for witness transactions)"),
		f.NewField("weight", arrow.PrimitiveTypes.Uint64, "The transaction's weight (between vsize*4-3 and vsize*4)"),
		f.NewField("version", arrow.PrimitiveTypes.Uint64, "The version"),
		f.NewField("lock_time", arrow.PrimitiveTypes.Uint64, "The lock time"),
		f.NewField("is_coinbase", arrow.FixedWidthTypes.Boolean, "True if this transaction is a coinbase transaction"),
		f.NewField("index", arrow.PrimitiveTypes.Uint64, "The transaction index"),
		f.NewField("block", newBlockDataType(), "The block header"),
		f.NewField("inputs", arrow.ListOf(newTransactionInputDataType()), "The inputs"),
		f.NewField("outputs", arrow.ListOf(newTransactionOutputDataType()), "The outputs"),
		f.NewField("input_count", arrow.PrimitiveTypes.Uint64, "The number of inputs"),
		f.NewField("output_count", arrow.PrimitiveTypes.Uint64, "The number of outputs"),
		f.NewField("input_value", arrow.PrimitiveTypes.Uint64, "Total value of inputs"),
		f.NewField("output_value", arrow.PrimitiveTypes.Uint64, "Total value of outputs"),
		f.NewField("fee", arrow.PrimitiveTypes.Uint64, "The fee paid by this transaction"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
	return transaction
}

func newBlockSchema() *arrow.Schema {
	f := xarrow.NewSchemaFactory()
	return f.NewSchema(
		f.NewField("hash", arrow.BinaryTypes.String, "The block hash"),
		f.NewField("size", arrow.PrimitiveTypes.Uint64, "The block size"),
		f.NewField("stripped_size", arrow.PrimitiveTypes.Uint64, "The block size excluding witness data"),
		f.NewField("weight", arrow.PrimitiveTypes.Uint64, "The block weight as defined in BIP 141"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block height or number"),
		f.NewField("version", arrow.PrimitiveTypes.Uint64, "The block version"),
		f.NewField("merkle_root", arrow.BinaryTypes.String, "The root node of a Merkle tree, where leaves are transaction hashes"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The block creation time expressed in UNIX epoch time"),
		f.NewField("nonce", arrow.PrimitiveTypes.Uint64, "The median block time expressed in UNIX epoch time"),
		f.NewField("bits", arrow.BinaryTypes.String, "The bits"),
		f.NewField("difficulty", arrow.BinaryTypes.String, "The difficulty"),
		f.NewField("chain_work", arrow.BinaryTypes.String, "Expected number of hashes required to produce the chain up to this block (in hex)"),
		f.NewField("transaction_count", arrow.PrimitiveTypes.Uint64, "The number of transactions in the block"),
		f.NewField("previous_block_hash", arrow.BinaryTypes.String, "The hash of the previous block"),
		f.NewField("next_block_hash", arrow.BinaryTypes.String, "The hash of the next block"),
		f.NewField("transactions", arrow.ListOf(arrow.BinaryTypes.String), "The list of transaction hashes"),
		f.NewField("_partition_by", arrow.PrimitiveTypes.Uint64, "Records with the same _partition_by value will be stored in the same s3 directory"),
		f.NewField("_repartition_by_range", arrow.PrimitiveTypes.Uint64, "Records will be range partitioned base on the _repartition_by_range column"),
	)
}

func newBlockDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("hash", arrow.BinaryTypes.String, "The block hash"),
		f.NewField("size", arrow.PrimitiveTypes.Uint64, "The block size"),
		f.NewField("stripped_size", arrow.PrimitiveTypes.Uint64, "The block size excluding witness data"),
		f.NewField("weight", arrow.PrimitiveTypes.Uint64, "The block weight as defined in BIP 141"),
		f.NewField("number", arrow.PrimitiveTypes.Uint64, "The block height or number"),
		f.NewField("version", arrow.PrimitiveTypes.Uint64, "The block version"),
		f.NewField("merkle_root", arrow.BinaryTypes.String, "The root node of a Merkle tree, where leaves are transaction hashes"),
		f.NewField("timestamp", arrow.PrimitiveTypes.Uint64, "The block creation time expressed in UNIX epoch time"),
		f.NewField("nonce", arrow.PrimitiveTypes.Uint64, "The median block time expressed in UNIX epoch time"),
		f.NewField("bits", arrow.BinaryTypes.String, "The bits"),
		f.NewField("difficulty", arrow.BinaryTypes.String, "The difficulty"),
		f.NewField("chain_work", arrow.BinaryTypes.String, "Expected number of hashes required to produce the chain up to this block (in hex)"),
		f.NewField("transaction_count", arrow.PrimitiveTypes.Uint64, "The number of transactions in the block"),
		f.NewField("previous_block_hash", arrow.BinaryTypes.String, "The hash of the previous block"),
		f.NewField("next_block_hash", arrow.BinaryTypes.String, "The hash of the next block"),
	)
}

func newTransactionInputDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("index", arrow.PrimitiveTypes.Uint64, "Zero-indexed number of an input within a transaction"),
		f.NewField("coinbase", arrow.BinaryTypes.String, "The coinbase is the content of the 'input' of a generation transaction."),
		f.NewField("spent_transaction_hash", arrow.BinaryTypes.String, "The hash of the spent transaction"),
		f.NewField("spent_output_index", arrow.PrimitiveTypes.Uint64, "The output index of the spent transaction"),
		f.NewField("script_asm", arrow.BinaryTypes.String, "Symbolic representation of the bitcoin's script language op-codes"),
		f.NewField("script_hex", arrow.BinaryTypes.String, "Hexadecimal representation of the bitcoin's script language op-codes"),
		f.NewField("sequence", arrow.PrimitiveTypes.Uint64, "The script sequence number"),
		f.NewField("transaction_input_witnesses", arrow.ListOf(arrow.BinaryTypes.String), "hex-encoded witness data"),
		f.NewField("type", arrow.BinaryTypes.String, "The address type of the spent output"),
		f.NewField("address", arrow.BinaryTypes.String, "The address which owns the spent output"),
		f.NewField("value", arrow.PrimitiveTypes.Uint64, "The value in base currency attached to the spent output"),
	)
}

func newTransactionOutputDataType() arrow.DataType {
	f := xarrow.NewSchemaFactory()
	return f.NewStruct(
		f.NewField("index", arrow.PrimitiveTypes.Uint64, "Zero-indexed number of an output within a transaction used by a later transaction to refer to that specific output"),
		f.NewField("script_asm", arrow.BinaryTypes.String, "Symbolic representation of the bitcoin's script language op-codes"),
		f.NewField("script_hex", arrow.BinaryTypes.String, "Hexadecimal representation of the bitcoin's script language op-codes"),
		f.NewField("type", arrow.BinaryTypes.String, "The address type of the output"),
		f.NewField("address", arrow.BinaryTypes.String, "The address which owns this output"),
		f.NewField("value", arrow.PrimitiveTypes.Uint64, "The value in base currency attached to this output"),
	)
}
