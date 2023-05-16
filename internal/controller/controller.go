package controller

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainsformer/internal/controller/internal"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
)

type (
	Controller = internal.Controller
	Handler    = internal.Handler

	ControllerParams struct {
		fx.In
		fxparams.Params
		Ethereum    Controller `name:"ethereum"`
		Bitcoin     Controller `name:"bitcoin"`
		EthereumRaw Controller `name:"ethereum_raw"`
		Rosetta     Controller `name:"rosetta"`
	}
)

// NewController
// The Ethereum controller defines schemas for most evm chains.
// The EthereumRaw controller defines transaction schema with a string typed value_raw field which mitigates the issue of value overflow for some evm chains.
// The Bitcoin controller defines schemas for the Bitcoin network.
// The Rosetta controller defines rosetta schemas for networks that support rosetta parsing.
func NewController(params ControllerParams) (Controller, error) {
	switch blockchain := params.Config.Blockchain(); blockchain {
	case common.Blockchain_BLOCKCHAIN_ETHEREUM,
		common.Blockchain_BLOCKCHAIN_POLYGON,
		common.Blockchain_BLOCKCHAIN_OPTIMISM:
		return params.Ethereum, nil
	case common.Blockchain_BLOCKCHAIN_BITCOIN:
		return params.Bitcoin, nil
	case common.Blockchain_BLOCKCHAIN_BSC,
		common.Blockchain_BLOCKCHAIN_ARBITRUM:
		return params.EthereumRaw, nil
	default:
		return nil, xerrors.Errorf("controller is not implemented: %v", blockchain)
	}
}
