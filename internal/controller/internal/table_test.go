package internal

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainsformer/internal/controller/internal/constant"
)

type (
	tableTestSuite struct {
		suite.Suite
		ctrl *gomock.Controller
	}
)

func TestTableSuite(t *testing.T) {
	suite.Run(t, new(tableTestSuite))
}

func (s *tableTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
}

func (s *tableTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *tableTestSuite) TestTableAttributes() {
	defaultAttributes := NewTableAttributes("test")
	s.Require().Equal(&TableAttributes{
		TableName: "test",
	}, defaultAttributes)

	withTagAttributes := NewTableAttributes("test1")
	s.Require().Equal(&TableAttributes{
		TableName: "test1",
	}, withTagAttributes)

	withFormatAttributes := NewTableAttributes("test2", WithFormat(constant.TableFormatRosetta))
	s.Require().Equal(&TableAttributes{
		TableName:   "test2",
		TableFormat: constant.TableFormatRosetta,
	}, withFormatAttributes)

	withEncodingAttributes := NewTableAttributes("test3", WithEncoding(constant.EncodingRaw))
	s.Require().Equal(&TableAttributes{
		TableName: "test3",
		Encoding:  constant.EncodingRaw,
	}, withEncodingAttributes)

	withAllAttributes := NewTableAttributes(
		"test4",
		WithFormat(constant.TableFormatRosetta),
		WithEncoding(constant.EncodingRaw),
	)
	s.Require().Equal(&TableAttributes{
		TableName:   "test4",
		TableFormat: constant.TableFormatRosetta,
		Encoding:    constant.EncodingRaw,
	}, withAllAttributes)
}
