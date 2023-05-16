package xarrow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecimal128FromString(t *testing.T) {
	tests := []struct {
		value string
		valid bool
	}{
		{
			value: "123",
			valid: true,
		},
		{
			value: "12345678901234567890",
			valid: true,
		},
		{
			value: "-123",
			valid: true,
		},
		{
			value: "",
			valid: true,
		},
		{
			value: "123.0",
			valid: false,
		},
		{
			value: "0x3",
			valid: false,
		},
		{
			value: "99999999999999999999999999999999999999",
			valid: true,
		},
		{
			value: "-99999999999999999999999999999999999999",
			valid: true,
		},
		{
			value: "100000000000000000000000000000000000000",
			valid: false,
		},
		{
			value: "-100000000000000000000000000000000000000",
			valid: false,
		},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			require := require.New(t)

			actual, err := Decimal128FromString(test.value)
			if test.valid {
				require.NoError(err)
				expected := "0"
				if test.value != "" {
					expected = test.value
				}
				require.Equal(expected, actual.BigInt().String())
			} else {
				require.Error(err)
			}
		})
	}
}
