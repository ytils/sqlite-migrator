package migrator

import (
	"fmt"
	"regexp"
	"time"
)

// IDFunc defines a function that validates the migration filename and extracts its numeric ID.
// The ID must be unique and increasing with each new migration.
type IDFunc func(filename string) (uint32, error)

var (
	reMigrationFilename = regexp.MustCompile(`^(\d+)_.*\.sql$`)
	epoch               = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
)

type migrationFile struct {
	ID       uint32
	Filename string
	SQL      string
}

// DefaultIDFunc is the default IDFunc used by the migrator.
// It expects the filename to be in the YYYYMMDDHHMMSS_<description>.sql format
// and defines the ID as the number of seconds since 2023-01-01 00:00:00 UTC.
func DefaultIDFunc(filename string) (uint32, error) {
	matches := reMigrationFilename.FindStringSubmatch(filename)
	if len(matches) < 2 { // nolint:gomnd // matches[0] is the full string
		return 0, fmt.Errorf("%s, expected YYYYMMDDHHMMSS_<description>.sql", filename)
	}
	idTime, err := time.Parse("20060102150405", matches[1])
	if err != nil {
		return 0, fmt.Errorf("%s, expected YYYYMMDDHHMMSS_<description>.sql", filename)
	}
	return uint32(idTime.Unix() - epoch), nil
}
