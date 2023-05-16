package controller

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/bitcoin"
	"github.com/coinbase/chainsformer/internal/controller/ethereum"
	"github.com/coinbase/chainsformer/internal/controller/internal"
	"github.com/coinbase/chainsformer/internal/controller/rosetta"
)

var Module = fx.Options(
	fx.Provide(NewController),
	fx.Provide(internal.NewHandler),
	bitcoin.Module,
	ethereum.Module,
	rosetta.Module,
)
