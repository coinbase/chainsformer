package rosetta

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/rosetta/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "rosetta",
		Target: NewController,
	}),
	tables.Module,
)
