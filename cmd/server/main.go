package main

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/sdk/services"

	"github.com/coinbase/chainsformer/internal/chainstorage"
	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/controller"
	"github.com/coinbase/chainsformer/internal/server"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
	"github.com/coinbase/chainsformer/internal/utils/tally"
)

func main() {
	manager := startManager()
	manager.WaitForInterrupt()
}

func startManager(opts ...fx.Option) services.SystemManager {
	manager := services.NewManager()
	ctx := manager.Context()
	logger := manager.Logger()

	opts = append(
		opts,
		chainstorage.Module,
		config.Module,
		controller.Module,
		server.Module,
		fxparams.Module,
		tally.Module,
		fx.NopLogger,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Invoke(server.NewServer),
	)

	app := fx.New(opts...)

	if err := app.Start(ctx); err != nil {
		logger.Fatal("failed to start app", zap.Error(err))
	}
	manager.AddPreShutdownHook(func() {
		logger.Info("shutting down server")
		if err := app.Stop(ctx); err != nil {
			logger.Error("failed to stop app", zap.Error(err))
		}
	})

	logger.Info("started app")
	return manager
}
