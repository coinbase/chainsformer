package errors

import "golang.org/x/xerrors"

var (
	ErrInvalidArgument    = xerrors.New("invalid argument")
	ErrNotFound           = xerrors.New("not found")
	ErrClientStreamClosed = xerrors.New("client stream closed")

	TransportClosingErrMsg = "transport is closing"
)
