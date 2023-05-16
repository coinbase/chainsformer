package controller

import (
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/utils/testapp"
	"github.com/coinbase/chainsformer/internal/utils/testutil"
)

func TestNewController(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		var controller Controller
		testapp.New(
			t,
			testapp.WithConfig(cfg),
			Module,
			chainstorage.Module,
			fx.Populate(&controller),
		)

		require.NotNil(controller)
	})
}
