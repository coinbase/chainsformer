package tables

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "rosetta",
		Target: NewRosettaTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "rosetta",
		Target: NewRosettaBlocksTable,
	}),
)
