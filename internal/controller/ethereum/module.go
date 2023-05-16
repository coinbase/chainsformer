package ethereum

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/ethereum/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum",
		Target: NewController,
	}),
	tables.Module,
)
