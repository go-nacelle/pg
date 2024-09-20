package pgutil

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/go-nacelle/log/v2"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t testing.TB) DB {
	return NewTestDBWithLogger(t, log.NewNilLogger())
}

func NewTestDBWithLogger(t testing.TB, logger log.Logger) DB {
	t.Helper()

	id, err := randomHexString(16)
	require.NoError(t, err)

	var (
		testDatabaseName           = fmt.Sprintf("pgutil-test-%s", id)
		quotedTestDatabaseName     = pq.QuoteIdentifier(testDatabaseName)
		quotedTemplateDatabaseName = pq.QuoteIdentifier(os.Getenv("TEMPLATEDB"))

		// NOTE: Must interpolate identifiers here as placeholders aren't valid in this position.
		createDatabaseQuery       = queryf("CREATE DATABASE %s TEMPLATE %s", quotedTestDatabaseName, quotedTemplateDatabaseName)
		dropDatabaseQuery         = queryf("DROP DATABASE %s", quotedTestDatabaseName)
		terminateConnectionsQuery = Query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = {:name}", Args{"name": testDatabaseName})
	)

	// Resolve "control" database URL
	baseURL := BuildDatabaseURL()
	parsedURL, err := url.Parse(baseURL)
	require.NoError(t, err)

	// Resolve "test" database URL
	testDBURL := parsedURL.ResolveReference(&url.URL{
		Path:     "/" + testDatabaseName,
		RawQuery: parsedURL.RawQuery,
	})

	// Open "control" database
	rawDB, err := sql.Open("postgres", baseURL)
	require.NoError(t, err)
	rawLoggingDB := newLoggingDB(rawDB, log.NewNilLogger())

	// Create "test" database
	require.NoError(t, rawLoggingDB.Exec(context.Background(), createDatabaseQuery))

	// Open "test" database
	testDB, err := sql.Open("postgres", testDBURL.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		defer rawDB.Close()

		require.NoError(t, testDB.Close())
		require.NoError(t, rawLoggingDB.Exec(context.Background(), terminateConnectionsQuery))
		require.NoError(t, rawLoggingDB.Exec(context.Background(), dropDatabaseQuery))
	})

	return newLoggingDB(testDB, logger)
}
