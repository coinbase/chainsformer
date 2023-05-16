package chainstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainsformer/internal/utils/testutil"

	sdkmocks "github.com/coinbase/chainstorage/sdk/mocks"
)

type (
	sessionTestSuite struct {
		suite.Suite
		ctrl       *gomock.Controller
		session    *sessionImpl
		sdkSession *sdkmocks.MockSession
		client     *sdkmocks.MockClient
	}
)

func TestSessionTestSuite(t *testing.T) {
	suite.Run(t, new(sessionTestSuite))
}

func (s *sessionTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.sdkSession = sdkmocks.NewMockSession(s.ctrl)
	s.client = sdkmocks.NewMockClient(s.ctrl)

	s.session = &sessionImpl{
		sdkSession: s.sdkSession,
	}

	// Use the mocked client for ChainStorage requests.
	s.sdkSession.EXPECT().Client().Return(s.client).AnyTimes()
}

func (s *sessionTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *sessionTestSuite) TestGetTipHeight_Failed_GetChainMetadata() {
	require := testutil.Require(s.T())

	errorGetChainMetadata := xerrors.New("failed to get chain metadata")
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(nil, errorGetChainMetadata)

	tip, err := s.session.GetTipHeight(context.Background())
	require.Equal(uint64(0), tip)
	require.True(errors.Is(err, errorGetChainMetadata))
}

func (s *sessionTestSuite) TestGetTipHeight_Failed_GetLatestBlock() {
	require := testutil.Require(s.T())

	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(&chainstorageapi.GetChainMetadataResponse{}, nil)
	errorGetLatestBlock := xerrors.New("failed to get latest block")
	blockHeight := uint64(0)
	s.client.EXPECT().GetLatestBlock(gomock.Any()).Return(blockHeight, errorGetLatestBlock)

	tip, err := s.session.GetTipHeight(context.Background())
	require.Equal(blockHeight, tip)
	require.True(errors.Is(err, errorGetLatestBlock))
}

func (s *sessionTestSuite) TestGetTipHeight_Success() {
	require := testutil.Require(s.T())

	chainMeta := chainstorageapi.GetChainMetadataResponse{
		IrreversibleDistance: 100,
	}
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(&chainMeta, nil)
	blockHeight := uint64(120)
	s.client.EXPECT().GetLatestBlock(gomock.Any()).Return(blockHeight, nil)

	tip, err := s.session.GetTipHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(20), tip)
}

func (s *sessionTestSuite) TestGetStartHeight_Failed_GetChainMetadata() {
	require := testutil.Require(s.T())

	errorGetChainMetadata := xerrors.New("failed to get chain metadata")
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(nil, errorGetChainMetadata)

	height, err := s.session.GetStartHeight(context.Background())
	require.Equal(uint64(0), height)
	require.True(errors.Is(err, errorGetChainMetadata))
}

func (s *sessionTestSuite) TestGetStartHeight_Success() {
	require := testutil.Require(s.T())

	chainMeta := chainstorageapi.GetChainMetadataResponse{
		BlockStartHeight: 100,
	}
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(&chainMeta, nil)
	height, err := s.session.GetStartHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(100), height)
}

func (s *sessionTestSuite) TestGetEventSequenceByPosition_InvalidInputs() {
	require := testutil.Require(s.T())

	seq, err := s.session.GetEventSequenceByPosition(context.Background(), "dummy")
	require.Equal(int64(0), seq)
	require.Contains(err.Error(), "invalid event position")
}

func (s *sessionTestSuite) TestGetEventSequenceByPosition_Failed_GetChainEvents() {
	require := testutil.Require(s.T())

	errorGetChainEvents := xerrors.New("failed to get chain event")
	eventPos := EarliestEventPosition
	s.client.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: eventPos,
		EventTag:                0,
	}).Return(nil, errorGetChainEvents)

	seq, err := s.session.GetEventSequenceByPosition(context.Background(), eventPos)
	require.Equal(int64(0), seq)
	require.True(errors.Is(err, errorGetChainEvents))
}

func (s *sessionTestSuite) TestGetEventSequenceByPosition_Success() {
	require := testutil.Require(s.T())

	eventPos := EarliestEventPosition
	expectedEvnets := []*chainstorageapi.BlockchainEvent{
		{
			SequenceNum: 100,
		},
	}

	s.client.EXPECT().GetChainEvents(gomock.Any(), &chainstorageapi.GetChainEventsRequest{
		InitialPositionInStream: eventPos,
		EventTag:                0,
	}).Return(expectedEvnets, nil)

	seq, err := s.session.GetEventSequenceByPosition(context.Background(), eventPos)
	require.NoError(err)
	require.Equal(int64(100), seq)
}

func (s *sessionTestSuite) TestGetChainMetadata_Failure() {
	require := testutil.Require(s.T())

	errorGetChainMetadata := xerrors.New("failed to get chain metadata")
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(nil, errorGetChainMetadata)

	res, err := s.session.GetStaticChainMetadata(context.Background(), &chainstorageapi.GetChainMetadataRequest{})
	require.Nil(res)
	require.True(errors.Is(err, errorGetChainMetadata))
}

func (s *sessionTestSuite) TestGetChainMetadata_Success() {
	require := testutil.Require(s.T())

	chainMeta := &chainstorageapi.GetChainMetadataResponse{
		BlockStartHeight: 100,
	}
	s.client.EXPECT().GetStaticChainMetadata(gomock.Any(), &chainstorageapi.GetChainMetadataRequest{}).Return(chainMeta, nil)
	res, err := s.session.GetStaticChainMetadata(context.Background(), &chainstorageapi.GetChainMetadataRequest{})
	require.NoError(err)
	require.Equal(chainMeta, res)
}
