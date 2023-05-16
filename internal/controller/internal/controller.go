package internal

const (
	packageName = "controller"
)

type (
	Controller interface {
		Tables() []Table
	}
)
