package database

import (
	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil"
)

func CreateRunner(databaseURL string, migrationDirectory string, logger log.Logger) (*pgutil.Runner, error) {
	if migrationDirectory == "" {
		panic("migration directory is not set by called command")
	}

	db, err := Dial(databaseURL, logger)
	if err != nil {
		return nil, err
	}

	reader := pgutil.NewFilesystemMigrationReader(migrationDirectory)
	runner, err := pgutil.NewMigrationRunner(db, reader, logger)
	if err != nil {
		return nil, err
	}

	return runner, nil
}
