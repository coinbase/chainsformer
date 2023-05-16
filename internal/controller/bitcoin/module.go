package bitcoin

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/bitcoin/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "bitcoin",
		Target: NewController,
	}),
	tables.Module,
)
