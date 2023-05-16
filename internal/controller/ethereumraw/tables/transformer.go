package tables

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	ethereumTables "github.com/coinbase/chainsformer/internal/controller/ethereum/tables"
	"github.com/coinbase/chainsformer/internal/utils/partition"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func (t transactionsTable) transformTransactions(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, partitionBySize uint64) error {
	header := block.Header
	if header == nil {
		return xerrors.New("header is required")
	}

	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for _, transaction := range transactions {
		xarrow.NewRecordAppender(recordBuilder).
			AppendString(transaction.Hash).
			AppendUint64(transaction.Index).
			AppendString(transaction.BlockHash).
			AppendUint64(transaction.BlockNumber).
			AppendUint64(uint64(transaction.BlockTimestamp.GetSeconds())).
			AppendString(transaction.From).
			AppendString(transaction.To).
			AppendUint64(transaction.Nonce).
			AppendString(transaction.Value).
			AppendUint64(transaction.Gas).
			AppendUint64(transaction.GasPrice).
			AppendString(transaction.Input).
			AppendUint64(transaction.Type).
			AppendUint64(transaction.GetMaxFeePerGas()).
			AppendUint64(transaction.GetMaxPriorityFeePerGas()).
			AppendUint64(transaction.GetPriorityFeePerGas()).
			AppendStruct(func(sa *xarrow.StructAppender) {
				ethereumTables.TransformBlock(sa, header)
			}).
			AppendStruct(func(sa *xarrow.StructAppender) {
				ethereumTables.TransformReceipt(sa, transaction, t.config)
			}).
			AppendList(func(la *xarrow.ListAppender) {
				transformTraces(la, transaction)
			}).
			AppendUint64(partition.GetPartitionByNumber(header.Number, partitionBySize)).
			AppendUint64(header.Number).
			Build()
	}

	return nil
}

func (t nativeStreamedTransactionsTable) transformStreamedTransactions(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, event *chainstorageapi.BlockchainEvent, partitionBySize uint64) error {
	header := block.Header
	if header == nil {
		return xerrors.New("header is required")
	}

	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for _, transaction := range transactions {
		xarrow.NewRecordAppender(recordBuilder).
			AppendInt64(event.GetSequenceNum()).
			AppendString(event.GetType().String()).
			AppendString(transaction.Hash).
			AppendUint64(transaction.Index).
			AppendString(transaction.BlockHash).
			AppendUint64(transaction.BlockNumber).
			AppendUint64(uint64(transaction.BlockTimestamp.GetSeconds())).
			AppendString(transaction.From).
			AppendString(transaction.To).
			AppendUint64(transaction.Nonce).
			AppendString(transaction.Value).
			AppendUint64(transaction.Gas).
			AppendUint64(transaction.GasPrice).
			AppendString(transaction.Input).
			AppendUint64(transaction.Type).
			AppendUint64(transaction.GetMaxFeePerGas()).
			AppendUint64(transaction.GetMaxPriorityFeePerGas()).
			AppendUint64(transaction.GetPriorityFeePerGas()).
			AppendStruct(func(sa *xarrow.StructAppender) {
				ethereumTables.TransformBlock(sa, header)
			}).
			AppendStruct(func(sa *xarrow.StructAppender) {
				ethereumTables.TransformReceipt(sa, transaction, t.config)
			}).
			AppendList(func(la *xarrow.ListAppender) {
				transformTraces(la, transaction)
			}).
			AppendUint64(partition.GetPartitionByNumber(uint64(event.GetSequenceNum()), partitionBySize)).
			AppendUint64(uint64(event.GetSequenceNum())).
			Build()
	}

	return nil
}

func transformTraces(la *xarrow.ListAppender, transaction *chainstorageapi.EthereumTransaction) {
	for _, trace := range transaction.FlattenedTraces {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendString(trace.TransactionHash).
				AppendUint64(trace.TransactionIndex).
				AppendString(trace.BlockHash).
				AppendUint64(trace.BlockNumber).
				AppendString(trace.From).
				AppendString(trace.To).
				AppendString(trace.Value).
				AppendString(trace.Input).
				AppendString(trace.Output).
				AppendString(trace.Type).
				AppendString(trace.TraceType).
				AppendString(trace.CallType).
				AppendUint64(trace.Gas).
				AppendUint64(trace.GasUsed).
				AppendUint64(trace.Subtraces).
				AppendList(func(la *xarrow.ListAppender) {
					for _, v := range trace.TraceAddress {
						la.AppendUint64(v)
					}
				}).
				AppendString(trace.Error).
				AppendUint64(trace.Status).
				AppendString(trace.TraceId)
		})
	}
}
