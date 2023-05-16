package server

import (
	"context"
	"time"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/coinbase/chainstorage/sdk/services"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/coinbase/chainsformer/internal/config"
	"github.com/coinbase/chainsformer/internal/controller"
	"github.com/coinbase/chainsformer/internal/utils/fxparams"
	"github.com/coinbase/chainsformer/internal/utils/log"
)

type (
	ServerParams struct {
		fx.In
		fxparams.Params
		Lifecycle fx.Lifecycle
		Manager   services.SystemManager
		Handler   controller.Handler
	}

	server struct {
		manager services.SystemManager
		logger  *zap.Logger
		config  *config.Config
		server  flight.Server
	}
)

func NewServer(params ServerParams) (*server, error) {
	logger := log.WithPackage(params.Logger)
	unaryInt := grpc_middleware.ChainUnaryServer(
	// XXX: Add your own interceptors here.
	)

	streamInt := grpc_middleware.ChainStreamServer(
	// XXX: Add your own interceptors here.
	)

	middleware := []flight.ServerMiddleware{
		{
			Unary:  unaryInt,
			Stream: streamInt,
		},
	}

	flightServer := flight.NewServerWithMiddleware(
		middleware,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    5 * time.Second,
			Timeout: 5 * time.Second,
		}),
	)
	s := &server{
		manager: params.Manager,
		logger:  logger,
		server:  flightServer,
		config:  params.Config,
	}
	s.registerServices(params.Handler)
	params.Lifecycle.Append(fx.Hook{
		OnStart: s.onStart,
		OnStop:  s.onStop,
	})
	return s, nil
}

func (s *server) onStart(ctx context.Context) error {
	s.logger.Info("starting server")
	addr := s.config.Server.BindAddress
	if err := s.server.Init(addr); err != nil {
		return xerrors.Errorf("failed to init server at %v: %w", addr, err)
	}

	s.daemonizeServer(s.manager)
	s.logger.Info("started server")
	return nil
}

func (s *server) onStop(ctx context.Context) error {
	s.logger.Info("stopping server")
	s.server.Shutdown()
	return nil
}

func (s *server) registerServices(svc flight.FlightServer) {
	s.server.RegisterFlightService(svc)

	// Register reflection to enable grpcurl.
	reflection.Register(s.server)

	// Register health check.
	grpc_health_v1.RegisterHealthServer(s.server, health.NewServer())
}

func (s *server) daemonizeServer(manager services.SystemManager) {
	runGRPCServer := func(ctx context.Context) (services.ShutdownFunction, chan error) {
		errorChannel := make(chan error)
		done := make(chan struct{})

		go func() {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					errorChannel <- xerrors.Errorf("recovered from panic: %+v", r)
				}
			}()

			if err := s.server.Serve(); err != nil {
				errorChannel <- xerrors.Errorf("failed to run server: %w", err)
				return
			}
		}()

		return func(ctx context.Context) error {
			s.server.Shutdown()
			<-done
			return nil
		}, errorChannel
	}

	manager.ServiceWaitGroup().Add(1)
	go func() {
		defer manager.ServiceWaitGroup().Done()
		services.Daemonize(manager, runGRPCServer, "GRPC Server")
	}()
}
