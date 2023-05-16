package config_test

import (
	"sort"
	"testing"

	"golang.org/x/exp/slices"

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
