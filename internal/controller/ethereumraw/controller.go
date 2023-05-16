package ethereumraw

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/controller/internal"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
)

type (
	ControllerParams struct {
		fx.In
		fxparams.Params
		Tables []internal.Table `group:"ethereum_raw"`
	}

	controller struct {
		tables []internal.Table
	}
)

func NewController(params ControllerParams) internal.Controller {
	var tables []internal.Table
	supportedFormats := params.Config.Table.GetSupportedFormats()
	for _, table := range params.Tables {
		if supportedFormats[table.GetFormat().String()] {
			tables = append(tables, table)
		}
	}

	return &controller{
		tables: tables,
	}
}

func (c *controller) Tables() []internal.Table {
	return c.tables
}
