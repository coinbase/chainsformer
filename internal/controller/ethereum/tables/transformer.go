package tables

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainsformer/internal/config"
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
		value, err := xarrow.Decimal128FromString(transaction.Value)
		if err != nil {
			return xerrors.Errorf("failed to convert value to decimal128 (hash=%v): %w", transaction.Hash, err)
		}

		xarrow.NewRecordAppender(recordBuilder).
			AppendString(transaction.Hash).
			AppendUint64(transaction.Index).
			AppendString(transaction.BlockHash).
			AppendUint64(transaction.BlockNumber).
			AppendUint64(uint64(transaction.BlockTimestamp.GetSeconds())).
			AppendString(transaction.From).
			AppendString(transaction.To).
			AppendUint64(transaction.Nonce).
			AppendDecimal128(value).
			AppendUint64(transaction.Gas).
			AppendUint64(transaction.GasPrice).
			AppendString(transaction.Input).
			AppendUint64(transaction.Type).
			AppendUint64(transaction.GetMaxFeePerGas()).
			AppendUint64(transaction.GetMaxPriorityFeePerGas()).
			AppendUint64(transaction.GetPriorityFeePerGas()).
			AppendStruct(func(sa *xarrow.StructAppender) {
				TransformBlock(sa, header)
			}).
			AppendStruct(func(sa *xarrow.StructAppender) {
				TransformReceipt(sa, transaction, t.config)
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

func (t blocksTable) transformBlocks(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, partitionBySize uint64) error {
	header := block.GetHeader()
	if header == nil {
		return xerrors.New("header is required")
	}

	totalDifficulty, err := xarrow.Decimal128FromString(header.TotalDifficulty)
	if err != nil {
		return xerrors.Errorf("failed to convert total difficulty to decimal128 (hash=%v): %w", header.Hash, err)
	}

	ra := xarrow.NewRecordAppender(recordBuilder).
		AppendString(header.Hash).
		AppendString(header.ParentHash).
		AppendUint64(header.Number).
		AppendString(header.Nonce).
		AppendString(header.Sha3Uncles).
		AppendString(header.LogsBloom).
		AppendString(header.TransactionsRoot).
		AppendString(header.StateRoot).
		AppendString(header.ReceiptsRoot).
		AppendString(header.Miner).
		AppendUint64(header.Difficulty).
		AppendDecimal128(totalDifficulty).
		AppendUint64(header.Size).
		AppendString(header.ExtraData).
		AppendUint64(header.GasLimit).
		AppendUint64(header.GasUsed).
		AppendUint64(uint64(header.Timestamp.GetSeconds())).
		AppendUint64(uint64(len(header.Transactions))).
		AppendUint64(header.GetBaseFeePerGas()).
		AppendList(func(la *xarrow.ListAppender) {
			for _, transaction := range header.Transactions {
				la.AppendString(transaction)
			}
		}).
		AppendList(func(la *xarrow.ListAppender) {
			for _, uncle := range header.Uncles {
				la.AppendString(uncle)
			}
		}).
		AppendList(func(la *xarrow.ListAppender) {
			for _, uncle := range block.Uncles {
				la.AppendStruct(func(sa *xarrow.StructAppender) {
					TransformBlock(sa, uncle)
				})
			}
		})

	switch t.config.Blockchain() {
	case common.Blockchain_BLOCKCHAIN_ETHEREUM:
		ra.AppendList(func(la *xarrow.ListAppender) {
			transformWithdrawals(la, header)
		}).AppendString(header.WithdrawalsRoot)
	}

	ra.AppendUint64(partition.GetPartitionByNumber(header.Number, partitionBySize)).
		AppendUint64(header.Number).
		Build()
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
		value, err := xarrow.Decimal128FromString(transaction.Value)
		if err != nil {
			return xerrors.Errorf("failed to convert value to decimal128 (hash=%v): %w", transaction.Hash, err)
		}

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
			AppendDecimal128(value).
			AppendUint64(transaction.Gas).
			AppendUint64(transaction.GasPrice).
			AppendString(transaction.Input).
			AppendUint64(transaction.Type).
			AppendUint64(transaction.GetMaxFeePerGas()).
			AppendUint64(transaction.GetMaxPriorityFeePerGas()).
			AppendUint64(transaction.GetPriorityFeePerGas()).
			AppendStruct(func(sa *xarrow.StructAppender) {
				TransformBlock(sa, header)
			}).
			AppendStruct(func(sa *xarrow.StructAppender) {
				TransformReceipt(sa, transaction, t.config)
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

func (t nativeStreamedBlocksTable) transformStreamedBlocks(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, event *chainstorageapi.BlockchainEvent, partitionBySize uint64) error {
	header := block.GetHeader()
	if header == nil {
		return xerrors.New("header is required")
	}

	totalDifficulty, err := xarrow.Decimal128FromString(header.TotalDifficulty)
	if err != nil {
		return xerrors.Errorf("failed to convert total difficulty to decimal128 (hash=%v): %w", header.Hash, err)
	}

	ra := xarrow.NewRecordAppender(recordBuilder).
		AppendInt64(event.GetSequenceNum()).
		AppendString(event.GetType().String()).
		AppendString(header.Hash).
		AppendString(header.ParentHash).
		AppendUint64(header.Number).
		AppendString(header.Nonce).
		AppendString(header.Sha3Uncles).
		AppendString(header.LogsBloom).
		AppendString(header.TransactionsRoot).
		AppendString(header.StateRoot).
		AppendString(header.ReceiptsRoot).
		AppendString(header.Miner).
		AppendUint64(header.Difficulty).
		AppendDecimal128(totalDifficulty).
		AppendUint64(header.Size).
		AppendString(header.ExtraData).
		AppendUint64(header.GasLimit).
		AppendUint64(header.GasUsed).
		AppendUint64(uint64(header.Timestamp.GetSeconds())).
		AppendUint64(uint64(len(header.Transactions))).
		AppendUint64(header.GetBaseFeePerGas()).
		AppendList(func(la *xarrow.ListAppender) {
			for _, transaction := range header.Transactions {
				la.AppendString(transaction)
			}
		}).
		AppendList(func(la *xarrow.ListAppender) {
			for _, uncle := range header.Uncles {
				la.AppendString(uncle)
			}
		}).
		AppendList(func(la *xarrow.ListAppender) {
			for _, uncle := range block.Uncles {
				la.AppendStruct(func(sa *xarrow.StructAppender) {
					TransformBlock(sa, uncle)
				})
			}
		})

	switch t.config.Blockchain() {
	case common.Blockchain_BLOCKCHAIN_ETHEREUM:
		ra.AppendList(func(la *xarrow.ListAppender) {
			transformWithdrawals(la, header)
		}).AppendString(header.WithdrawalsRoot)
	}

	ra.AppendUint64(partition.GetPartitionByNumber(uint64(event.GetSequenceNum()), partitionBySize)).
		AppendUint64(uint64(event.GetSequenceNum())).
		Build()

	return nil
}

func (t rawNativeStreamedTransactionsTable) transformRawStreamedTransactions(recordBuilder *array.RecordBuilder, block *chainstorageapi.EthereumBlock, event *chainstorageapi.BlockchainEvent, partitionBySize uint64) error {
	header := block.Header
	if header == nil {
		return xerrors.New("header is required")
	}

	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for _, transaction := range transactions {
		data, err := proto.Marshal(transaction)
		if err != nil {
			return xerrors.New("transaction failed to marshal into protobuf")
		}

		xarrow.NewRecordAppender(recordBuilder).
			AppendInt64(event.GetSequenceNum()).
			AppendString(event.GetType().String()).
			AppendString(transaction.Hash).
			AppendUint64(transaction.Index).
			AppendString(transaction.BlockHash).
			AppendUint64(transaction.BlockNumber).
			AppendUint64(uint64(transaction.BlockTimestamp.GetSeconds())).
			AppendBinary(data).
			AppendUint64(partition.GetPartitionByNumber(uint64(event.GetSequenceNum()), partitionBySize)).
			AppendUint64(uint64(event.GetSequenceNum())).
			Build()
	}

	return nil
}

func TransformBlock(sa *xarrow.StructAppender, header *chainstorageapi.EthereumHeader) {
	sa.AppendString(header.Hash).
		AppendString(header.ParentHash).
		AppendUint64(header.Number).
		AppendUint64(uint64(header.Timestamp.GetSeconds())).
		AppendString(header.Miner).
		AppendUint64(header.Difficulty).
		AppendUint64(header.GasLimit).
		AppendUint64(header.GasUsed).
		AppendUint64(header.GetBaseFeePerGas())
}

func TransformReceipt(sa *xarrow.StructAppender, transaction *chainstorageapi.EthereumTransaction, config *config.Config) {
	receipt := transaction.Receipt
	sa.AppendString(receipt.TransactionHash).
		AppendUint64(receipt.TransactionIndex).
		AppendString(receipt.BlockHash).
		AppendUint64(receipt.BlockNumber).
		AppendString(receipt.From).
		AppendString(receipt.To).
		AppendUint64(receipt.CumulativeGasUsed).
		AppendUint64(receipt.GasUsed).
		AppendString(receipt.ContractAddress).
		AppendList(func(la *xarrow.ListAppender) {
			transformLogs(la, receipt)
		}).
		AppendString(receipt.LogsBloom).
		AppendString(receipt.Root).
		AppendUint64(receipt.Type).
		AppendUint64(receipt.GetStatus()).
		AppendUint64(receipt.GetEffectiveGasPrice())

	l1GasUsed := uint64(0)
	l1GasPrice := uint64(0)
	l1Fee := uint64(0)
	l1FeeScalar := ""
	if receipt.GetL1FeeInfo() != nil {
		l1GasUsed = receipt.GetL1FeeInfo().L1GasUsed
		l1GasPrice = receipt.GetL1FeeInfo().L1GasPrice
		l1Fee = receipt.GetL1FeeInfo().L1Fee
		l1FeeScalar = receipt.GetL1FeeInfo().L1FeeScalar
	}

	switch config.Blockchain() {
	case common.Blockchain_BLOCKCHAIN_ARBITRUM:
		sa.AppendUint64(l1GasUsed)
	case common.Blockchain_BLOCKCHAIN_OPTIMISM:
		sa.AppendUint64(l1GasUsed).
			AppendUint64(l1GasPrice).
			AppendUint64(l1Fee).
			AppendString(l1FeeScalar)
	}
}

func transformLogs(la *xarrow.ListAppender, receipt *chainstorageapi.EthereumTransactionReceipt) {
	for _, log := range receipt.Logs {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(log.LogIndex).
				AppendString(log.TransactionHash).
				AppendUint64(log.TransactionIndex).
				AppendString(log.BlockHash).
				AppendUint64(log.BlockNumber).
				AppendString(log.Address).
				AppendString(log.Data).
				AppendList(func(la *xarrow.ListAppender) {
					for _, topic := range log.Topics {
						la.AppendString(topic)
					}
				}).
				AppendBool(log.Removed)
		})
	}
}

func transformTraces(la *xarrow.ListAppender, transaction *chainstorageapi.EthereumTransaction) {
	for _, trace := range transaction.FlattenedTraces {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			// FIXME: error handling
			value, _ := xarrow.Decimal128FromString(trace.Value)

			sa.AppendString(trace.TransactionHash).
				AppendUint64(trace.TransactionIndex).
				AppendString(trace.BlockHash).
				AppendUint64(trace.BlockNumber).
				AppendString(trace.From).
				AppendString(trace.To).
				AppendDecimal128(value).
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

func transformWithdrawals(la *xarrow.ListAppender, header *chainstorageapi.EthereumHeader) {
	for _, withdrawal := range header.Withdrawals {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(withdrawal.Index).
				AppendUint64(withdrawal.ValidatorIndex).
				AppendString(withdrawal.Address).
				AppendUint64(withdrawal.Amount)
		})
	}
}
