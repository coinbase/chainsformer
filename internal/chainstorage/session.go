package chainstorage

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainsformer/internal/utils/fxparams"
)

type (
	// Session is an interface which interacts with ChainStorage.
	Session interface {
		Client() sdk.Client
		Parser() sdk.Parser

		GetTipHeight(ctx context.Context) (uint64, error)
		GetStartHeight(ctx context.Context) (uint64, error)
		GetEventSequenceByPosition(ctx context.Context, eventPosition string) (int64, error)

		// Fetch the static chain metadata from ChainStorage. Calling this instead of GetChainMetadata can avoid
		// the impact of configuration changes made at ChainStorage on the fly.
		GetStaticChainMetadata(ctx context.Context, req *chainstorageapi.GetChainMetadataRequest) (*chainstorageapi.GetChainMetadataResponse, error)
	}

	Params struct {
		fx.In
		fxparams.Params
		Manager sdk.SystemManager
	}

	// This struct interacts with ChainStorage.
	sessionImpl struct {
		sdkSession sdk.Session
	}
)

const (
	EarliestEventPosition = "EARLIEST"
	LatestEventPosition   = "LATEST"
)

func NewSession(params Params) (Session, error) {
	cfg := &params.Config.ChainStorageSDK.Config
	session, err := sdk.New(params.Manager, cfg)
	if err != nil {
		return nil, xerrors.Errorf("failed to create chainstorage session {%+v}: %w", cfg, err)
	}

	return &sessionImpl{
		sdkSession: session,
	}, nil
}

func (s *sessionImpl) Client() sdk.Client {
	return s.sdkSession.Client()
}

func (s *sessionImpl) Parser() sdk.Parser {
	return s.sdkSession.Parser()
}

func (s *sessionImpl) GetTipHeight(ctx context.Context) (uint64, error) {
	meta, err := s.GetStaticChainMetadata(ctx, &chainstorageapi.GetChainMetadataRequest{})
	if err != nil {
		return 0, xerrors.Errorf("failed to get chain metadata: %w", err)
	}
	tipHeight, err := s.sdkSession.Client().GetLatestBlock(ctx)
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest block: %w", err)
	}
	return tipHeight - meta.GetIrreversibleDistance(), nil
}

func (s *sessionImpl) GetStartHeight(ctx context.Context) (uint64, error) {
	meta, err := s.GetStaticChainMetadata(ctx, &chainstorageapi.GetChainMetadataRequest{})
	if err != nil {
		return 0, xerrors.Errorf("failed to get chain metadata: %w", err)
	}
	return meta.GetBlockStartHeight(), nil
}

func (s *sessionImpl) GetEventSequenceByPosition(ctx context.Context, eventPosition string) (int64, error) {
	if eventPosition != EarliestEventPosition && eventPosition != LatestEventPosition {
		return 0, xerrors.Errorf("invalid event position")
	}

	events, err := s.sdkSession.Client().GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: eventPosition,
	})
	if err != nil || len(events) != 1 {
		return 0, xerrors.Errorf("failed to get chain event: %w", err)
	}

	return events[0].SequenceNum, nil
}

func (s *sessionImpl) GetStaticChainMetadata(ctx context.Context, req *chainstorageapi.GetChainMetadataRequest) (*chainstorageapi.GetChainMetadataResponse, error) {
	return s.sdkSession.Client().GetStaticChainMetadata(ctx, req)
}
