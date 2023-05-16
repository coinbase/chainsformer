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
	rosettaBlocksTable struct{}
)

func NewRosettaBlocksTable(params internal.CommonTableParams) internal.Table {
	return internal.NewBatchTable(
		&params,
		internal.NewTableAttributes(internal.TableNameBlocks, internal.WithFormat(constant.TableFormatRosetta)),
		newBlockSchema(),
		rosettaBlocksTable{},
	)
}

func (t rosettaBlocksTable) TransformBlock(ctx context.Context, block *chainstorageapi.Block, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error {
	rosettaBlock, err := parser.ParseRosettaBlock(ctx, block)
	if err != nil {
		return xerrors.Errorf("failed to parse raw block to rosetta block: %w", err)
	}

	rosettaBlockData := rosettaBlock.GetBlock()
	if rosettaBlockData == nil {
		return xerrors.New("failed to extract ethereum block from rosetta block")
	}

	if err := transformBlocks(recordBuilder, rosettaBlockData, partitionBySize); err != nil {
		return xerrors.Errorf("failed to transform blocks: %w", err)
	}

	return nil
}
