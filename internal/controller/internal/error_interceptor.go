package internal

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainsformer/internal/errors"
)

type (
	GrpcError interface {
		Error() string
		GRPCStatus() *status.Status
	}

	errorInterceptor struct {
		flight.BaseFlightServer
		next Handler
	}
)

func withErrorInterceptor(next Handler) Handler {
	return &errorInterceptor{
		next: next,
	}
}

func (i *errorInterceptor) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	return i.mapError(i.next.ListFlights(c, fs))
}

func (i *errorInterceptor) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	schema, err := i.next.GetSchema(ctx, in)

	return schema, i.mapError(err)
}

func (i *errorInterceptor) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	flightInfo, err := i.next.GetFlightInfo(ctx, in)

	return flightInfo, i.mapError(err)
}

func (i *errorInterceptor) DoAction(action *flight.Action, fs flight.FlightService_DoActionServer) error {
	return i.mapError(i.next.DoAction(action, fs))
}

func (i *errorInterceptor) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	return i.mapError(i.next.DoGet(tkt, fs))
}

func (i *errorInterceptor) mapError(err error) error {
	if err == nil {
		return nil
	}

	description := "internal error"
	code := codes.Internal

	var grpcErr GrpcError
	if xerrors.Is(err, context.Canceled) {
		description = "context cancelled"
		code = codes.Canceled
	} else if xerrors.Is(err, errors.ErrInvalidArgument) {
		description = "invalid request parameter"
		code = codes.InvalidArgument
	} else if xerrors.Is(err, errors.ErrNotFound) {
		description = "not found"
		code = codes.NotFound
	} else if xerrors.Is(err, errors.ErrClientStreamClosed) {
		description = "client stream closed"
		code = codes.Canceled
	} else if xerrors.As(err, &grpcErr) {
		// If the error is already a grpc error, use the given code.
		description = code.String()
		code = grpcErr.GRPCStatus().Code()
	}

	return status.Errorf(code, "%v: %+v", description, err)
}
