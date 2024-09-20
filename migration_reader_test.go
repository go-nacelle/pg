package pgutil

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUpQuery = `-- Create the comments table
CREATE TABLE comments (
    id         SERIAL PRIMARY KEY,
    post_id    INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content    TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
`

var testDownQuery = `-- Drop the comments table
DROP TABLE IF EXISTS comments;
`

var testConcurrentIndexUpQuery = `-- Create a concurrent index
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);`

var testConcurrentIndexDownQuery = `-- Drop the concurrent index
DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;`

func TestReadMigrations(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		definitions, err := ReadMigrations(NewFilesystemMigrationReader(path.Join("testdata", "migrations", "valid")))
		require.NoError(t, err)
		require.Len(t, definitions, 3)

		assert.Equal(t, Definition{
			ID:        3,
			Name:      "third",
			UpQuery:   RawQuery(testUpQuery),
			DownQuery: RawQuery(testDownQuery),
		}, definitions[2])
	})

	t.Run("CIC pattern", func(t *testing.T) {
		t.Skip()
		definitions, err := ReadMigrations(NewFilesystemMigrationReader(path.Join("testdata", "migrations", "cic_pattern")))
		require.NoError(t, err)
		require.Len(t, definitions, 4)

		assert.Equal(t, Definition{
			ID:        3,
			Name:      "third",
			UpQuery:   RawQuery(testConcurrentIndexUpQuery),
			DownQuery: RawQuery(testConcurrentIndexDownQuery),
			IndexMetadata: &IndexMetadata{
				TableName: "users",
				IndexName: "idx_users_email",
			},
		}, definitions[3])
	})

	t.Run("duplicate identifiers", func(t *testing.T) {
		_, err := ReadMigrations(NewFilesystemMigrationReader(path.Join("testdata", "migrations", "duplicate_identifiers")))
		assert.ErrorContains(t, err, "duplicate migration identifier 2")
	})

	t.Run("CIC with additional queries", func(t *testing.T) {
		_, err := ReadMigrations(NewFilesystemMigrationReader(path.Join("testdata", "migrations", "cic_with_additional_queries")))
		assert.ErrorContains(t, err, `"create index concurrently" is not the only statement in the up migration`)
	})

	t.Run("CIC in down migration", func(t *testing.T) {
		_, err := ReadMigrations(NewFilesystemMigrationReader(path.Join("testdata", "migrations", "cic_in_down_migration")))
		assert.ErrorContains(t, err, `"create index concurrently" is not the only statement in the up migration`)
	})
}
