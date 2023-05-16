package internal

import (
	"context"
	"strings"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainsformer/internal/errors"
	"github.com/coinbase/chainsformer/internal/utils/instrument"
)

type (
	instrumentInterceptor struct {
		flight.BaseFlightServer
		next                    Handler
		instrumentListFlights   instrument.Call
		instrumentGetSchema     instrument.Call
		instrumentGetFlightInfo instrument.Call
		instrumentDoAction      instrument.Call
		instrumentDoGet         instrument.Call
	}

	ListFlightsServer struct {
		flight.FlightService_ListFlightsServer
		ctx context.Context
	}

	DoActionServer struct {
		flight.FlightService_DoActionServer
		ctx context.Context
	}

	DoGetServer struct {
		flight.FlightService_DoGetServer
		ctx context.Context
	}
)

func withInstrumentInterceptor(next Handler, scope tally.Scope, logger *zap.Logger) Handler {
	scope = scope.SubScope("handler")
	return &instrumentInterceptor{
		next:                    next,
		instrumentListFlights:   newInstrument("list_flights", scope, logger),
		instrumentGetSchema:     newInstrument("get_schema", scope, logger),
		instrumentGetFlightInfo: newInstrument("get_flight_info", scope, logger),
		instrumentDoAction:      newInstrument("do_action", scope, logger),
		instrumentDoGet:         newInstrument("do_get", scope, logger),
	}
}

func newInstrument(method string, scope tally.Scope, logger *zap.Logger) instrument.Call {
	tags := map[string]string{
		"method": method,
	}
	scope = scope.Tagged(tags)
	logger = logger.With(
		zap.String("method", method),
	)
	return instrument.NewCall(
		scope,
		"request",
		instrument.WithLogger(logger, "handler.request"),
		instrument.WithTracer("handler.request", tags),
	)
}

func (i *instrumentInterceptor) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	return i.instrumentListFlights.Instrument(fs.Context(), func(ctx context.Context) error {
		return i.next.ListFlights(c, ListFlightsServer{fs, ctx})
	})
}

func (i *instrumentInterceptor) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	var result *flight.SchemaResult
	err := i.instrumentGetSchema.Instrument(ctx, func(ctx context.Context) error {
		schema, err := i.next.GetSchema(ctx, in)
		if err != nil {
			return err
		}

		result = schema
		return nil
	})

	return result, err
}

func (i *instrumentInterceptor) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var result *flight.FlightInfo
	err := i.instrumentGetFlightInfo.Instrument(ctx, func(ctx context.Context) error {
		flightInfo, err := i.next.GetFlightInfo(ctx, in)
		if err != nil {
			return err
		}

		result = flightInfo
		return nil
	})

	return result, err
}

func (i *instrumentInterceptor) DoAction(action *flight.Action, fs flight.FlightService_DoActionServer) error {
	return i.instrumentDoAction.Instrument(fs.Context(), func(ctx context.Context) error {
		return i.next.DoAction(action, DoActionServer{fs, ctx})
	})
}

func (i *instrumentInterceptor) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	return i.instrumentDoGet.Instrument(fs.Context(), func(ctx context.Context) error {
		return i.next.DoGet(tkt, DoGetServer{fs, ctx})
	})
}

func (s ListFlightsServer) Context() context.Context {
	return s.ctx
}

func (s DoActionServer) Context() context.Context {
	return s.ctx
}

func (s DoGetServer) Context() context.Context {
	return s.ctx
}

func (s DoGetServer) Send(fd *flight.FlightData) error {
	if err := s.SendMsg(fd); err != nil {
		if code := status.Code(err); code == codes.Unavailable || (code == codes.Unknown && strings.Contains(err.Error(), errors.TransportClosingErrMsg)) {
			err = xerrors.Errorf("%v: %w", err.Error(), errors.ErrClientStreamClosed)
		}

		return err
	}

	return nil
}
