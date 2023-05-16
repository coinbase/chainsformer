package tables

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainsformer/internal/utils/partition"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func (t transactionsTable) transformTransactions(recordBuilder *array.RecordBuilder, block *chainstorageapi.BitcoinBlock, partitionBySize uint64) error {
	header := block.GetHeader()
	if header == nil {
		return xerrors.New("header is required")
	}

	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for _, transaction := range transactions {
		xarrow.NewRecordAppender(recordBuilder).
			AppendString(transaction.TransactionId). // DO NOT USE transaction.Hash.
			AppendUint64(transaction.Size).
			AppendUint64(transaction.VirtualSize).
			AppendUint64(transaction.Weight).
			AppendUint64(transaction.Version).
			AppendUint64(transaction.LockTime).
			AppendBool(transaction.IsCoinbase).
			AppendUint64(transaction.Index).
			AppendStruct(func(sa *xarrow.StructAppender) {
				transformBlock(sa, header)
			}).
			AppendList(func(la *xarrow.ListAppender) {
				transformInputs(la, transaction.Inputs)
			}).
			AppendList(func(la *xarrow.ListAppender) {
				transformOutputs(la, transaction.Outputs)
			}).
			AppendUint64(transaction.InputCount).
			AppendUint64(transaction.OutputCount).
			AppendUint64(transaction.InputValue).
			AppendUint64(transaction.OutputValue).
			AppendUint64(transaction.Fee).
			AppendUint64(partition.GetPartitionByNumber(header.Height, partitionBySize)).
			AppendUint64(header.Height).
			Build()
	}

	return nil
}

func (t blocksTable) transformBlocks(recordBuilder *array.RecordBuilder, block *chainstorageapi.BitcoinBlock, partitionBySize uint64) error {
	header := block.GetHeader()
	if header == nil {
		return xerrors.New("header is required")
	}

	xarrow.NewRecordAppender(recordBuilder).
		AppendString(header.Hash).
		AppendUint64(header.Size).
		AppendUint64(header.StrippedSize).
		AppendUint64(header.Weight).
		AppendUint64(header.Height).
		AppendUint64(header.Version).
		AppendString(header.MerkleRoot).
		AppendUint64(header.Time).
		AppendUint64(header.Nonce).
		AppendString(header.Bits).
		AppendString(header.Difficulty).
		AppendString(header.ChainWork).
		AppendUint64(header.NumberOfTransactions).
		AppendString(header.PreviousBlockHash).
		AppendString(header.NextBlockHash).
		AppendList(func(la *xarrow.ListAppender) {
			for _, transaction := range block.Transactions {
				la.AppendString(transaction.TransactionId)
			}
		}).
		AppendUint64(partition.GetPartitionByNumber(header.Height, partitionBySize)).
		AppendUint64(header.Height).
		Build()

	return nil
}

func transformBlock(sa *xarrow.StructAppender, header *chainstorageapi.BitcoinHeader) {
	sa.AppendString(header.Hash).
		AppendUint64(header.Size).
		AppendUint64(header.StrippedSize).
		AppendUint64(header.Weight).
		AppendUint64(header.Height).
		AppendUint64(header.Version).
		AppendString(header.MerkleRoot).
		AppendUint64(header.Time).
		AppendUint64(header.Nonce).
		AppendString(header.Bits).
		AppendString(header.Difficulty).
		AppendString(header.ChainWork).
		AppendUint64(header.NumberOfTransactions).
		AppendString(header.PreviousBlockHash).
		AppendString(header.NextBlockHash)
}

func transformInputs(la *xarrow.ListAppender, inputs []*chainstorageapi.BitcoinTransactionInput) {
	for i, input := range inputs {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(uint64(i)).
				AppendString(input.Coinbase).
				AppendString(input.TransactionId).
				AppendUint64(input.FromOutputIndex).
				AppendString(input.GetScriptSignature().GetAssembly()).
				AppendString(input.GetScriptSignature().GetHex()).
				AppendUint64(input.Sequence).
				AppendList(func(la *xarrow.ListAppender) {
					for _, transactionInputWitness := range input.TransactionInputWitnesses {
						la.AppendString(transactionInputWitness)
					}
				}).
				AppendString(input.GetFromOutput().GetScriptPublicKey().GetType()).
				AppendString(input.GetFromOutput().GetScriptPublicKey().GetAddress()).
				AppendUint64(input.GetFromOutput().GetValue())
		})
	}
}

func transformOutputs(la *xarrow.ListAppender, outputs []*chainstorageapi.BitcoinTransactionOutput) {
	for _, output := range outputs {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(output.Index).
				AppendString(output.GetScriptPublicKey().GetAssembly()).
				AppendString(output.GetScriptPublicKey().GetHex()).
				AppendString(output.GetScriptPublicKey().GetType()).
				AppendString(output.GetScriptPublicKey().GetAddress()).
				AppendUint64(output.Value)
		})
	}
}
