package tables

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/rosetta/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewBlocksTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewNativeStreamedTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewNativeStreamedBlocksTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: NewRawNativeStreamedTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: tables.NewRosettaTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: tables.NewRosettaBlocksTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum",
		Target: tables.NewRawRosettaStreamedTransactionsTable,
	}),
)
