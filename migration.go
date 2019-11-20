package pgutil

import (
	"database/sql"

	"github.com/go-nacelle/nacelle"
	migrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
)

type migrationLogger struct {
	Logger nacelle.Logger
}

func (m *migrationLogger) Printf(format string, v ...interface{}) {
	m.Logger.Debug(format, v...)
}

func (m *migrationLogger) Verbose() bool {
	return false
}

func runMigrations(
	db *sql.DB,
	sourceDriver source.Driver,
	migrationsTable string,
	schemaName string,
	logger nacelle.Logger,
) error {
	if sourceDriver == nil {
		return nil
	}

	databaseDriver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: migrationsTable,
		SchemaName:      schemaName,
	})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("pgutil-source", sourceDriver, "postgres", databaseDriver)
	if err != nil {
		return err
	}

	m.Log = &migrationLogger{logger}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		// TODO - determine if error occurs because current version is too new
		return err
	}

	return nil
}
