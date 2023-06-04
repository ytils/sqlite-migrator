package invalid

import (
	"embed"
)

//go:embed invalid_name.sql
var FileName embed.FS

//go:embed 000001_invalid_format.sql
var Format embed.FS
