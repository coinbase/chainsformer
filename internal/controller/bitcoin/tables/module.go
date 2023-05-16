package tables

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "bitcoin",
		Target: NewTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "bitcoin",
		Target: NewBlocksTable,
	}),
)
