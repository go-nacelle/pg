package pgutil

import (
	"database/sql"
	"os"
	"strings"

	"github.com/go-nacelle/nacelle"
	migrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
)

type migrationLogger struct {
	Logger nacelle.Logger
}

func (m *migrationLogger) Printf(format string, v ...interface{}) {
	m.Logger.Debug("migrate: "+strings.TrimSpace(format), v...)
}

func (m *migrationLogger) Verbose() bool {
	return false
}

func runMigrations(
	db *sql.DB,
	sourceDriver source.Driver,
	logger nacelle.Logger,
	migrationsTable string,
	schemaName string,
	failOnNewerMigrationVersion bool,
) error {
	if sourceDriver == nil {
		return nil
	}

	logger.Info("Running migrations")

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
		// migrate returns not-exists errors when the database version is newer
		// than the target version - this can happen during rolling restarts when
		// an old version of a process starts after a newer one has become active
		// for the first time. This should be generally harmless when following
		// best practices, but we'll give an escape-hatch to kill the older
		// processes in such an event.

		if !os.IsNotExist(err) || failOnNewerMigrationVersion {
			return err
		}

		version, _, err := m.Version()
		if err != nil {
			return err
		}

		logger.Warning("Current database schema is on a future version %s", version)
		return nil
	}

	logger.Info("Database schema is up to date")
	return nil
}
