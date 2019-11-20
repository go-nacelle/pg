# Nacelle Postgres Utilities [![GoDoc](https://godoc.org/github.com/go-nacelle/pgutil?status.svg)](https://godoc.org/github.com/go-nacelle/pgutil) [![CircleCI](https://circleci.com/gh/go-nacelle/pgutil.svg?style=svg)](https://circleci.com/gh/go-nacelle/pgutil) [![Coverage Status](https://coveralls.io/repos/github/go-nacelle/pgutil/badge.svg?branch=master)](https://coveralls.io/github/go-nacelle/pgutil?branch=master)

Postgres utilities for use with nacelle.

---

### Usage

This library creates a [sqlx](https://github.com/jmoiron/sqlx) connection wrapped in a nacelle [logger](https://nacelle.dev/docs/core/log). The supplied initializer adds this connection into the nacelle [service container](https://nacelle.dev/docs/core/service) under the key `db`. The initializer will block until a ping succeeds.

```go
func setup(processes nacelle.ProcessContainer, services nacelle.ServiceContainer) error {
    processes.RegisterInitializer(pgutil.NewInitializer())

    // additional setup
    return nil
}
```

This library uses [golang migrate](https://github.com/golang-migrate/migrate) to optionally run migrations on application startup. To configure migrations, supply a [source driver](https://github.com/golang-migrate/migrate#migration-sources) to the initializer, as follows.

```go
import (
    _ "github.com/golang-migrate/migrate/v4/source/file"
    "github.com/golang-migrate/migrate/v4/source"
)

func setup(processes nacelle.ProcessContainer, services nacelle.ServiceContainer) error {
    migrationSourceDriver, err := source.Open("file:///migrations")
	if err != nil {
		return err
	}

    processes.RegisterInitializer(pgutil.NewInitializer(
        pgutil.WithMigrationSourceDriver(migrationSourceDriver)
    ))

    // ...
}
```

### Configuration

The default service behavior can be configured by the following environment variables.

| Environment Variable            | Required | Default           | Description                                                                                          |
| ------------------------------- | -------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| DATABASE_URL                    | yes      |                   | The connection string of the remote database.                                                        |
| LOG_SQL_QUERIES                 |          | false             | Whether or not to log parameterized SQL queries.                                                     |
| MIGRATIONS_TABLE                |          | schema_migrations | The name of the migrations table.                                                                    |
| MIGRATIONS_SCHEMA_NAME          |          | default           | The name of the schema used during migrations.                                                       |
| FAIL_ON_NEWER_MIGRATION_VERSION |          | false             | If true, fail startup when the database migration version is newer than the known set of migrations. |
