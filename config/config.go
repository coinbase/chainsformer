package config

import (
	"embed"
)

//go:embed chainsformer/*
var ConfigFS embed.FS
