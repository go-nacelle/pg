# Nacelle Postgres Utilities [![GoDoc](https://godoc.org/github.com/go-nacelle/pgutil?status.svg)](https://godoc.org/github.com/go-nacelle/pgutil) [![CircleCI](https://circleci.com/gh/go-nacelle/pgutil.svg?style=svg)](https://circleci.com/gh/go-nacelle/pgutil) [![Coverage Status](https://coveralls.io/repos/github/go-nacelle/pgutil/badge.svg?branch=master)](https://coveralls.io/github/go-nacelle/pgutil?branch=master)

Postgres utilities for use with nacelle.

---

### Usage

This library creates a [sqlx](https://github.com/jmoiron/sqlx) connection wrapped in a nacelle [logger](https://github.com/go-nacelle/log). The supplied initializer adds this connection into the nacelle [service container](https://github.com/go-nacelle/service) under the key `db`. The initializer will block until a ping succeeds.

```go
func setup(processes nacelle.ProcessContainer, services nacelle.ServiceContainer) error {
    processes.RegisterInitializer(pgutil.NewInitializer())

    // additional setup
    return nil
}
```

### Configuration

The default service behavior can be configured by the following environment variables.

| Environment Variable | Required | Default | Description |
| -------------------- | -------- | ------- | ----------- |
| DATABASE_URL         | yes      |         | The connection string of the remote database. |
| LOG_SQL_QUERIES      |          | false   | Whether or not to log parameterized SQL queries. |
