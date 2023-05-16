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
	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/protoutil"
	"github.com/coinbase/chainsformer/internal/utils/syncgroup"
	"github.com/coinbase/chainsformer/internal/utils/xarrow"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

type (
	StreamTransformer interface {
		TransformBlock(ctx context.Context, blockAndEvent *BlockAndEvent, parser sdk.Parser, recordBuilder *array.RecordBuilder, partitionBySize uint64) error
	}

	StreamTable struct {
		*baseTable
		session     chainstorage.Session
		transformer StreamTransformer
		config      config.StreamTableConfig
	}

	BlockAndEvent struct {
		Block           *chainstorageapi.Block
		BlockChainEvent *chainstorageapi.BlockchainEvent
	}

	sequenceInfo struct {
		startSeq int64
		endSeq   int64
	}
)

func NewStreamTable(commonParams *CommonTableParams, attributes *TableAttributes, schema *arrow.Schema, transformer StreamTransformer, config config.StreamTableConfig) *StreamTable {
	return &StreamTable{
		baseTable:   newBaseTable(commonParams, attributes, schema),
		session:     commonParams.Session,
		transformer: transformer,
		config:      config,
	}
}

func (t *StreamTable) GetEndpoints(ctx context.Context, cmd *api.GetFlightInfoCmd) ([]*flight.FlightEndpoint, error) {
	var endpoints []*flight.FlightEndpoint
	err := t.instrumentGetEndpoints.Instrument(ctx, func(ctx context.Context) error {
		streamQuery := cmd.GetStreamQuery()
		if streamQuery == nil {
			return xerrors.Errorf("streamQuery is not provided: %w", errors.ErrInvalidArgument)
		}

		seqInfo, err := t.getSequenceInfo(ctx, streamQuery.GetStartSequence(), streamQuery.GetEndSequence())
		if err != nil {
			return xerrors.Errorf("failed to get sequence info: %w", err)
		}

		eventsPerPartition := defaultEventsPerPartition
		if streamQuery.GetEventsPerPartition() > 0 {
			eventsPerPartition = streamQuery.GetEventsPerPartition()
		}
		numEndpoints := uint64(math.Ceil(float64(seqInfo.endSeq-seqInfo.startSeq+1) / float64(eventsPerPartition)))
		if numEndpoints > maxNumOfEndpoints {
			return xerrors.Errorf("events per (partition=%d) is too small, resulted in %d endpoints: %w", eventsPerPartition, numEndpoints, errors.ErrInvalidArgument)
		}

		endpoints = make([]*flight.FlightEndpoint, 0, numEndpoints)
		for i := seqInfo.startSeq; i < seqInfo.endSeq; i += int64(eventsPerPartition) {
			ticket := proto.Clone(cmd).(*api.GetFlightInfoCmd)
			ticket.GetStreamQuery().StartSequence = i
			ticket.GetStreamQuery().EndSequence = i + int64(eventsPerPartition)
			if ticket.GetStreamQuery().EndSequence > seqInfo.endSeq {
				ticket.GetStreamQuery().EndSequence = seqInfo.endSeq
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

func (t *StreamTable) DoGet(ctx context.Context, cmd *api.GetFlightInfoCmd, tableWriter xarrow.TableWriter) error {
	return t.instrumentDoGet.Instrument(ctx, func(ctx context.Context) error {
		streamQuery := cmd.GetStreamQuery()
		if streamQuery == nil {
			return xerrors.Errorf("streamQuery is not provided: %w", errors.ErrInvalidArgument)
		}

		if streamQuery.StartSequence >= streamQuery.EndSequence {
			return xerrors.Errorf("(startSequence=%d) must be less than or equal to (endSequence=%d): %w", streamQuery.StartSequence, streamQuery.EndSequence, errors.ErrInvalidArgument)
		}

		eventsPerRecord := streamQuery.EventsPerRecord
		if eventsPerRecord == 0 {
			eventsPerRecord = DefaultEventsPerRecord
		}

		eventsWritten := uint64(0)
		for i := streamQuery.StartSequence; i < streamQuery.EndSequence; i += int64(eventsPerRecord) {
			currMiniBatchSize := eventsPerRecord
			if i+int64(eventsPerRecord) > streamQuery.EndSequence {
				currMiniBatchSize = uint64(streamQuery.EndSequence - i)
			}

			blockAndEvents, err := t.getBlocksAndEvents(ctx, i, currMiniBatchSize)
			if err != nil {
				return xerrors.Errorf("failed to get blocks and events: %w", err)
			}

			for _, blockAndEvent := range blockAndEvents {
				if err := t.transformer.TransformBlock(ctx, blockAndEvent, t.session.Parser(), tableWriter.RecordBuilder(), cmd.GetStreamQuery().PartitionBySize); err != nil {
					return xerrors.Errorf("failed to process block and event: %w", err)
				}

				eventsWritten += 1
				if eventsWritten >= eventsPerRecord {
					if err := tableWriter.Flush(); err != nil {
						return xerrors.Errorf("failed to write record: %w", err)
					}
					eventsWritten = 0
				}
				t.counterBlocksProcessed.Inc(1)
			}
		}

		return nil
	})
}

func (t *StreamTable) getSequenceInfo(ctx context.Context, startSequence int64, endSequence int64) (*sequenceInfo, error) {
	initialSeq, err := t.session.GetEventSequenceByPosition(ctx, chainstorage.EarliestEventPosition)
	if err != nil {
		return nil, xerrors.Errorf("failed to get event earliest sequence: %w", err)
	}

	latestSeq, err := t.session.GetEventSequenceByPosition(ctx, chainstorage.LatestEventPosition)
	if err != nil {
		return nil, xerrors.Errorf("failed to get event latest sequence: %w", err)
	}

	if startSequence > latestSeq {
		return nil, xerrors.Errorf("(startSequence=%d) must be less than (latestEventSequence=%d): %w", startSequence, latestSeq, errors.ErrInvalidArgument)
	} else if startSequence < initialSeq {
		startSequence = initialSeq
	}

	if endSequence < startSequence+1 && endSequence > 0 {
		return nil, xerrors.Errorf("(endSequence=%d) must be greater than (startSequence=%d): %w", endSequence, startSequence, errors.ErrInvalidArgument)
	} else if endSequence > latestSeq+1 || endSequence == 0 {
		endSequence = latestSeq + 1
	}

	return &sequenceInfo{
		startSeq: startSequence,
		endSeq:   endSequence,
	}, nil
}

func (t *StreamTable) getBlocksAndEvents(ctx context.Context, startSeq int64, miniBatchSize uint64) ([]*BlockAndEvent, error) {
	events, err := t.session.Client().GetChainEvents(
		ctx,
		&chainstorageapi.GetChainEventsRequest{
			SequenceNum:  startSeq - 1,
			MaxNumEvents: miniBatchSize,
		})
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain events: %w", err)
	}

	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(t.config.GetParallelism()))
	blockAndEvents := make([]*BlockAndEvent, len(events))
	for i, event := range events {
		event := event
		i := i
		group.Go(func() error {
			block, err := t.session.Client().GetBlockWithTag(ctx, event.Block.Tag, event.Block.Height, event.Block.Hash)
			if err != nil {
				return xerrors.Errorf("failed to get Block with (tag=%d) (height=%d) (hash=%s): %w", event.Block.Tag, event.Block.Height, event.Block.Hash, err)
			}

			blockAndEvents[i] = &BlockAndEvent{
				BlockChainEvent: event,
				Block:           block,
			}
			return nil
		})
	}

	err = group.Wait()
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from (startSequence=%d) to (endSequence=%d): %w", startSeq, startSeq+int64(miniBatchSize)-1, err)
	}

	return blockAndEvents, nil
}
