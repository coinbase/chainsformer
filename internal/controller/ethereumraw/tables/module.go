package tables

import (
	"go.uber.org/fx"

	ethereumTables "github.com/coinbase/chainsformer/internal/controller/ethereum/tables"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "ethereum_raw",
		Target: NewTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum_raw",
		Target: ethereumTables.NewBlocksTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum_raw",
		Target: NewNativeStreamedTransactionsTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum_raw",
		Target: ethereumTables.NewNativeStreamedBlocksTable,
	}),
	fx.Provide(fx.Annotated{
		Group:  "ethereum_raw",
		Target: ethereumTables.NewRawNativeStreamedTransactionsTable,
	}),
)
