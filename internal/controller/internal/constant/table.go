package constant

//go:generate go-enum -f=$GOFILE --marshal
type (
	// ENUM(native, rosetta)
	TableFormat int

	// ENUM(none, raw)
	Encoding int
)
