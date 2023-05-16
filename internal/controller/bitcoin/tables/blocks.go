package tables

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow/array"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/controller/internal"
)

type (
	blocksTable struct {
		config *config.Config
	}
)

func NewBlocksTable(params internal.CommonTableParams) internal.Table {
	return internal.NewBatchTable(
		&params,
		internal.NewTableAttributes(internal.TableNameBlocks),
		newBlockSchema(),
		blocksTable{
			params.Config,
		},
	)
}

func (t blocksTable) TransformBlock(ctx context.Context, block *chainstorageapi.Block, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error {
	nativeBlock, err := parser.ParseNativeBlock(ctx, block)
	if err != nil {
		return xerrors.Errorf("failed to parse raw block to native block: %w", err)
	}

	bitcoinBlock := nativeBlock.GetBitcoin()
	if bitcoinBlock == nil {
		return xerrors.New("failed to extract bitcoin block from native block")
	}

	if err := t.transformBlocks(recordBuilder, bitcoinBlock, partitionBySize); err != nil {
		return xerrors.Errorf("failed to transform blocks: %w", err)
	}

	return nil
}
