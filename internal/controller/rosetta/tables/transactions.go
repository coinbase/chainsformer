package tables

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow/array"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainsformer/internal/controller/internal"
	"github.com/coinbase/chainsformer/internal/controller/internal/constant"
)

type (
	rosettaTransactionsTable            struct{}
	rawRosettaStreamedTransactionsTable struct{}
)

func NewRawRosettaStreamedTransactionsTable(params internal.CommonTableParams) internal.Table {
	return internal.NewStreamTable(
		&params,
		internal.NewTableAttributes(internal.TableNameStreamedTransactions,
			internal.WithFormat(constant.TableFormatRosetta),
			internal.WithEncoding(constant.EncodingRaw)),
		newRawStreamedTransactionSchema(),
		rawRosettaStreamedTransactionsTable{},
		params.Params.Config.Table.StreamTable,
	)
}

func NewRosettaTransactionsTable(params internal.CommonTableParams) internal.Table {
	return internal.NewBatchTable(
		&params,
		internal.NewTableAttributes(internal.TableNameTransactions, internal.WithFormat(constant.TableFormatRosetta)),
		newTransactionSchema(),
		rosettaTransactionsTable{},
	)
}

func (t rosettaTransactionsTable) TransformBlock(ctx context.Context, block *chainstorageapi.Block, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error {
	rosettaBlock, err := parser.ParseRosettaBlock(ctx, block)
	if err != nil {
		return xerrors.Errorf("failed to parse raw block to rosetta block: %w", err)
	}

	rosettaBlockData := rosettaBlock.GetBlock()
	if rosettaBlockData == nil {
		return xerrors.New("failed to extract ethereum block from rosetta block")
	}

	if err := transformTransactions(recordBuilder, rosettaBlockData, partitionBySize); err != nil {
		return xerrors.Errorf("failed to transform transactions: %w", err)
	}

	return nil
}

func (t rawRosettaStreamedTransactionsTable) TransformBlock(ctx context.Context, blockAndEvent *internal.BlockAndEvent, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error {
	rosettaBlock, err := parser.ParseRosettaBlock(ctx, blockAndEvent.Block)
	if err != nil {
		return xerrors.Errorf("failed to parse raw block to rosetta block: %w", err)
	}

	block := rosettaBlock.GetBlock()
	if block == nil {
		return xerrors.New("failed to extract rosetta block from raw block")
	}

	if err := transformRawRosettaStreamedTransactions(recordBuilder, block, blockAndEvent.BlockChainEvent, partitionBySize); err != nil {
		return xerrors.Errorf("failed to transform transactions: %w", err)
	}

	return nil
}
