# Nacelle Postgres Utilities

[![PkgGoDev](https://pkg.go.dev/badge/badge/github.com/go-nacelle/pgutil.svg)](https://pkg.go.dev/github.com/go-nacelle/pgutil)
[![Build status](https://github.com/go-nacelle/pgutil/actions/workflows/test.yml/badge.svg)](https://github.com/go-nacelle/pgutil/actions/workflows/test.yml)
[![Latest release](https://img.shields.io/github/release/go-nacelle/pgutil.svg)](https://github.com/go-nacelle/pgutil/releases/)

Postgres utilities for use with nacelle.

---

### Usage

This library creates a Postgres connection wrapped in a nacelle [logger](https://nacelle.dev/docs/core/log). The supplied initializer adds this connection into the nacelle [service container](https://nacelle.dev/docs/core/service) under the key `db`. The initializer will block until a ping succeeds.

```go
func setup(processes nacelle.ProcessContainer, services nacelle.ServiceContainer) error {
    processes.RegisterInitializer(pgutil.NewInitializer())

    // additional setup
    return nil
}
```

### Configuration

The default service behavior can be configured by the following environment variables.

| Environment Variable            | Required | Default           | Description                                                                                          |
| ------------------------------- | -------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| DATABASE_URL                    | yes      |                   | The connection string of the remote database.                                                        |
| LOG_SQL_QUERIES                 |          | false             | Whether or not to log parameterized SQL queries.                                                     |
