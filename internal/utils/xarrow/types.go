package xarrow

import (
	"math/big"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
	"golang.org/x/xerrors"
)

const (
	// Ref: https://docs.databricks.com/sql/language-manual/data-types/decimal-type.html
	decimalPrecision = 38
)

var DecimalTypes = struct {
	Decimal128 arrow.FixedWidthDataType
}{
	Decimal128: &arrow.Decimal128Type{
		Precision: decimalPrecision,
		Scale:     0,
	},
}

func Decimal128FromString(v string) (decimal128.Num, error) {
	if v == "" {
		v = "0"
	}

	bi, ok := new(big.Int).SetString(v, 10)
	if !ok {
		return decimal128.Num{}, xerrors.Errorf("failed to convert value to big.Int: %v", v)
	}

	if bi.BitLen() > 127 {
		return decimal128.Num{}, xerrors.Errorf("value cannot be represented as decimal128: %v", v)
	}

	if abs := strings.TrimPrefix(v, "-"); len(abs) > decimalPrecision {
		return decimal128.Num{}, xerrors.Errorf("value cannot be represented as DECIMAL(%v, 0): %v", decimalPrecision, v)
	}

	return decimal128.FromBigInt(bi), nil
}
