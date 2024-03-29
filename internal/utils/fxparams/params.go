package fxparams

import (
	"math/rand"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainsformer/internal/config"
)

// Params provide the common dependencies.
// Usage:
//
//	MyParams struct {
//	  fx.In
//	  fxparams.Params
//	  ...
//	}
type Params struct {
	Config  *config.Config
	Logger  *zap.Logger
	Metrics tally.Scope
}

var Module = fx.Options(
	fx.Provide(func(config *config.Config, logger *zap.Logger, metrics tally.Scope) Params {
		rand.Seed(time.Now().UnixNano())
		return Params{
			Config:  config,
			Logger:  logger,
			Metrics: metrics,
		}
	}),
)
