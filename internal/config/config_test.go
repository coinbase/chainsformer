package config_test

import (
	"os"
	"sort"
	"testing"

	"golang.org/x/exp/slices"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/utils/testapp"
	"github.com/coinbase/chainsformer/internal/utils/testutil"
)

func TestConfig(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		require.NotEmpty(cfg.Blockchain())
		require.NotEmpty(cfg.Network())
		require.NotEmpty(cfg.Env())

		expectedMapConfigs := map[string]struct {
			supportedFormats []string
			streamTable      struct {
				parallelism int
			}
			server struct {
				bindAddress string
			}
		}{
			"bitcoin-mainnet": {
				supportedFormats: []string{"native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"ethereum-mainnet": {
				supportedFormats: []string{"rosetta", "native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"ethereum-goerli": {
				supportedFormats: []string{"rosetta", "native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"polygon-mainnet": {
				supportedFormats: []string{"rosetta", "native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"bsc-mainnet": {
				supportedFormats: []string{"native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"arbitrum-mainnet": {
				supportedFormats: []string{"native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},

			"optimism-mainnet": {
				supportedFormats: []string{"native"},
				streamTable: struct {
					parallelism int
				}{
					parallelism: 10,
				},
				server: struct {
					bindAddress string
				}{
					bindAddress: ":9090",
				},
			},
		}

		expectedMapConfig, ok := expectedMapConfigs[cfg.ConfigName]
		require.True(ok)

		sort.Strings(expectedMapConfig.supportedFormats)
		sort.Strings(cfg.Table.SupportedFormats)
		require.True(slices.Equal(expectedMapConfig.supportedFormats, cfg.Table.SupportedFormats))
		require.Equal(expectedMapConfig.streamTable.parallelism, cfg.Table.StreamTable.GetParallelism())
		require.Equal(expectedMapConfig.server.bindAddress, cfg.Server.BindAddress)
	})
}

func TestConfigOverrideConfigPath(t *testing.T) {
	require := testutil.Require(t)
	err := os.Setenv(config.EnvVarConfigPath, "../../config/chainsformer/ethereum/goerli/base.yml")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarConfigPath)

	cfg, err := config.New()
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, cfg.Blockchain())
	require.Equal(common.Network_NETWORK_ETHEREUM_GOERLI, cfg.Network())
}
