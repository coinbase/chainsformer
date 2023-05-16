package ethereumraw

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/ethereumraw/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum_raw",
		Target: NewController,
	}),
	tables.Module,
)
