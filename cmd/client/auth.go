package main

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow/flight"
)

type ClientAuth struct{}

func (a *ClientAuth) Authenticate(ctx context.Context, c flight.AuthConn) error {
	if err := c.Send([]byte{}); err != nil {
		return err
	}

	_, err := c.Read()
	return err
}

func (a *ClientAuth) GetToken(ctx context.Context) (string, error) {
	return "", nil
}
