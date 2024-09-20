package pgutil

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/hexops/autogold/v2"
	"github.com/stretchr/testify/require"
)

func TestDescribeSchema(t *testing.T) {
	var (
		goldenDir  = path.Join("testdata", "golden")
		schemaFile = path.Join("testdata", "schemas", "describe.sql")
	)

	schemaBytes, err := os.ReadFile(schemaFile)
	require.NoError(t, err)

	db := NewTestDB(t)
	err = db.Exec(context.Background(), RawQuery(string(schemaBytes)))
	require.NoError(t, err)

	schema, err := DescribeSchema(context.Background(), db)
	require.NoError(t, err)
	autogold.ExpectFile(t, schema, autogold.Dir(goldenDir))
}
