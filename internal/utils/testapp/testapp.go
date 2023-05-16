package testapp

import (
	"testing"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	"github.com/coinbase/chainstorage/sdk/services"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
	"github.com/coinbase/chainsformer/internal/utils/testutil"
	"github.com/coinbase/chainsformer/internal/utils/tracer"
)

type (
	TestApp interface {
		Close()
		Logger() *zap.Logger
		Config() *config.Config
	}

	TestFn func(t *testing.T, cfg *config.Config)

	testAppImpl struct {
		app    *fxtest.App
		logger *zap.Logger
		config *config.Config
	}

	localOnlyOption struct {
		fx.Option
	}
)

var (
	TestConfigs = []string{
		"bitcoin-mainnet",
		"ethereum-mainnet",
		"ethereum-goerli",
		"polygon-mainnet",
		"bsc-mainnet",
		"arbitrum-mainnet",
		"optimism-mainnet",
	}
)

func New(t testing.TB, opts ...fx.Option) TestApp {
	manager := services.NewMockSystemManager()

	var cfg *config.Config
	opts = append(
		opts,
		config.Module,
		fxparams.Module,
		tracer.Module,
		fx.NopLogger,
		fx.Provide(func() testing.TB { return t }),
		fx.Provide(func() *zap.Logger { return manager.Logger() }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Populate(&cfg),
	)

	app := fxtest.New(t, opts...)
	app.RequireStart()
	return &testAppImpl{
		app:    app,
		logger: manager.Logger(),
		config: cfg,
	}
}

// WithIntegration runs the test only if $TEST_TYPE is integration.
func WithIntegration() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsIntegrationTest() {
				logger.Warn("skipping integration test", zap.String("test", tb.Name()))
				tb.Skip()
			}
		}),
	}
}

// WithFunctional runs the test only if $TEST_TYPE is functional.
func WithFunctional() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsFunctionalTest() {
				logger.Warn("skipping functional test", zap.String("test", tb.Name()))
				tb.Skip()
			}
		}),
	}
}

// WithConfig overrides the default config.
func WithConfig(cfg *config.Config) fx.Option {
	return config.WithCustomConfig(cfg)
}

// WithBlockchainNetwork loads the config according to the specified blockchain and network.
func WithBlockchainNetwork(blockchain common.Blockchain, network common.Network) fx.Option {
	cfg, err := config.New(
		config.WithBlockchain(blockchain),
		config.WithNetwork(network),
	)
	if err != nil {
		panic(err)
	}

	return WithConfig(cfg)
}

func (a *testAppImpl) Close() {
	a.app.RequireStop()
}

func (a *testAppImpl) Logger() *zap.Logger {
	return a.logger
}

func (a *testAppImpl) Config() *config.Config {
	return a.config
}

var envsToTest = []config.Env{
	config.EnvLocal,
	config.EnvDevelopment,
	config.EnvProduction,
}

func TestAllEnvs(t *testing.T, fn TestFn) {
	for _, env := range envsToTest {
		t.Run(string(env), func(t *testing.T) {
			require := testutil.Require(t)

			cfg, err := config.New(config.WithEnvironment(env))
			require.NoError(err)
			require.Equal(env, cfg.Env())

			fn(t, cfg)
		})
	}
}

func TestAllConfigs(t *testing.T, fn TestFn) {
	for _, configName := range TestConfigs {
		t.Run(configName, func(t *testing.T) {
			for _, env := range envsToTest {
				t.Run(string(env), func(t *testing.T) {
					require := testutil.Require(t)
					blockchain, network, err := config.ParseConfigName(configName)
					require.NoError(err)

					cfg, err := config.New(
						config.WithEnvironment(env),
						config.WithBlockchain(blockchain),
						config.WithNetwork(network),
					)
					require.NoError(err)
					require.Equal(env, cfg.Env())
					require.Equal(blockchain, cfg.Blockchain())
					require.Equal(network, cfg.Network())

					fn(t, cfg)
				})
			}
		})
	}
}
