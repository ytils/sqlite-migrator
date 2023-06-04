package base

import (
	"embed"
)

//go:embed *.sql should-be-skipped/*
var FS embed.FS
