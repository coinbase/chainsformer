package internal

import (
	"context"
	"math"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	"github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

type (
	BatchTransformer interface {
		TransformBlock(ctx context.Context, block *chainstorageapi.Block, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error
	}

	BatchTable struct {
		*baseTable
		session     chainstorage.Session
		transformer BatchTransformer
	}
)

func NewBatchTable(commonParams *CommonTableParams, attributes *TableAttributes, schema *arrow.Schema, transformer BatchTransformer) *BatchTable {
	return &BatchTable{
		baseTable:   newBaseTable(commonParams, attributes, schema),
		session:     commonParams.Session,
		transformer: transformer,
	}
}

func (t *BatchTable) parseGetEndpointsParams(ctx context.Context, cmd *api.GetFlightInfoCmd) (uint64, uint64, uint64, error) {
	batchQuery := cmd.GetBatchQuery()
	if batchQuery == nil {
		return 0, 0, 0, xerrors.Errorf("batchQuery is not provided: %w", errors.ErrInvalidArgument)
	}

	startHeight := batchQuery.GetStartHeight()
	endHeight := batchQuery.GetEndHeight()
	blocksPerPartition := defaultBlocksPerPartition
	if batchQuery.GetBlocksPerPartition() > 0 {
		blocksPerPartition = batchQuery.GetBlocksPerPartition()
	}

	meta, err := t.session.GetStaticChainMetadata(ctx, &chainstorageapi.GetChainMetadataRequest{})
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to get chain metadata: %w", err)
	}
	chainStartHeight := meta.GetBlockStartHeight()
	if startHeight < chainStartHeight {
		startHeight = chainStartHeight
	}

	if endHeight == 0 {
		endHeight, err = t.session.GetTipHeight(ctx)
		if err != nil {
			return 0, 0, 0, xerrors.Errorf("failed to get tip height: %w", err)
		}
	}

	if endHeight <= startHeight {
		return 0, 0, 0, xerrors.Errorf("startHeight=%d, endHeight=%d: %w", startHeight, endHeight, errors.ErrInvalidArgument)
	}

	return startHeight, endHeight, blocksPerPartition, nil
}

func (t *BatchTable) GetEndpoints(ctx context.Context, cmd *api.GetFlightInfoCmd) ([]*flight.FlightEndpoint, error) {
	var endpoints []*flight.FlightEndpoint
	err := t.instrumentGetEndpoints.Instrument(ctx, func(ctx context.Context) error {
		startHeight, endHeight, blocksPerPartition, err := t.parseGetEndpointsParams(ctx, cmd)
		if err != nil {
			return xerrors.Errorf("failed to parse params from cmd(%+v): %w", cmd, err)
		}

		numEndpoints := uint64(math.Ceil(float64(endHeight-startHeight) / float64(blocksPerPartition)))
		if numEndpoints > maxNumOfEndpoints {
			return xerrors.Errorf("blocks per partition(%d) is too small, resulted in %d endpoints: %w", blocksPerPartition, numEndpoints, errors.ErrInvalidArgument)
		}

		endpoints = make([]*flight.FlightEndpoint, 0, numEndpoints)
		for i := startHeight; i < endHeight; i += blocksPerPartition {
			ticket := proto.Clone(cmd).(*api.GetFlightInfoCmd)
			ticket.GetBatchQuery().StartHeight = i
			ticket.GetBatchQuery().EndHeight = i + blocksPerPartition
			if ticket.GetBatchQuery().EndHeight > endHeight {
				ticket.GetBatchQuery().EndHeight = endHeight
			}

			ticketBytes, err := protoutil.MarshalJSON(ticket)
			if err != nil {
				return xerrors.Errorf("failed to marshal ticket(%+v): %w", ticket, err)
			}
			endpoints = append(endpoints, &flight.FlightEndpoint{
				Ticket: &flight.Ticket{
					Ticket: ticketBytes,
				},
			})
		}
		return nil
	})
	return endpoints, err
}

func (t *BatchTable) parseDoGetParams(ctx context.Context, cmd *api.GetFlightInfoCmd) (uint64, uint64, uint64, error) {
	batchQuery := cmd.GetBatchQuery()
	if batchQuery == nil {
		return 0, 0, 0, xerrors.Errorf("batchQuery is not provided: %w", errors.ErrInvalidArgument)
	}

	startHeight := batchQuery.GetStartHeight()
	endHeight := batchQuery.GetEndHeight()
	blocksPerRecord := DefaultBlocksPerRecord
	if batchQuery.GetBlocksPerRecord() > 0 {
		blocksPerRecord = batchQuery.GetBlocksPerRecord()
	}

	if endHeight <= startHeight {
		return 0, 0, 0, xerrors.Errorf("startHeight=%d, endHeight=%d: %w", startHeight, endHeight, errors.ErrInvalidArgument)
	}
	return startHeight, endHeight, blocksPerRecord, nil
}

func (t *BatchTable) DoGet(ctx context.Context, cmd *api.GetFlightInfoCmd, tableWriter xarrow.TableWriter) error {
	return t.instrumentDoGet.Instrument(ctx, func(ctx context.Context) error {
		startHeight, endHeight, blocksPerRecord, err := t.parseDoGetParams(ctx, cmd)
		if err != nil {
			return xerrors.Errorf("failed to parse params from cmd(%+v): %w", cmd, err)
		}

		blocksWritten := uint64(0)
		for chunkStart := startHeight; chunkStart < endHeight; chunkStart += blocksPerRecord {
			chunkEnd := chunkStart + blocksPerRecord
			if chunkEnd > endHeight {
				chunkEnd = endHeight
			}

			blocks, err := t.session.Client().GetBlocksByRange(ctx, chunkStart, chunkEnd)
			if err != nil {
				return xerrors.Errorf("failed to get raw blocks: %w", err)
			}
			for _, block := range blocks {
				if err := t.transformer.TransformBlock(ctx, block, t.session.Parser(), tableWriter.RecordBuilder(), cmd.GetBatchQuery().PartitionBySize); err != nil {
					return xerrors.Errorf("failed to process block: %w", err)
				}

				blocksWritten += 1
				if blocksWritten >= blocksPerRecord {
					if err := tableWriter.Flush(); err != nil {
						return xerrors.Errorf("failed to write record: %w", err)
					}
					blocksWritten = 0
				}
				t.counterBlocksProcessed.Inc(1)
			}
		}

		return nil
	})
}
