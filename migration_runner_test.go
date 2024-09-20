package pgutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-nacelle/log/v2"
	"github.com/stretchr/testify/require"
)

func TestApply(t *testing.T) {
	definitions := []RawDefinition{
		{ID: 1, RawUpQuery: "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);"},
		{ID: 2, RawUpQuery: "INSERT INTO users (email) VALUES ('test@gmail.com');"},
		{ID: 3, RawUpQuery: "ALTER TABLE users ADD COLUMN name TEXT;"},
		{ID: 4, RawUpQuery: "UPDATE users SET name = 'test';"},
		{ID: 5, RawUpQuery: "CREATE UNIQUE INDEX users_email_idx ON users (email);"},
	}
	definitionsWithoutUpdates := []RawDefinition{definitions[0], definitions[1], definitions[2], definitions[4]}
	reader := MigrationReaderFunc(func() ([]RawDefinition, error) { return definitions, nil })
	readerWithoutUpdates := MigrationReaderFunc(func() ([]RawDefinition, error) { return definitionsWithoutUpdates, nil })

	t.Run("all", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply all migrations from scratch
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert last migration (unique index) was applied
		err = db.Exec(ctx, Query(
			"INSERT INTO users (name, email) VALUES ({:name}, {:email})",
			Args{"name": "duplicate", "email": "test@gmail.com"},
		))
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})

	t.Run("tail", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Head first
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.Apply(ctx, 2))

		// Assert no name column yet
		_, _, err = ScanString(db.Query(ctx, RawQuery("SELECT name FROM users WHERE email = 'test@gmail.com'")))
		require.ErrorContains(t, err, "column \"name\" does not exist")

		// Apply the tail
		require.NoError(t, runner.Apply(ctx, 5))

		// Assert name column added and populated
		email, _, err := ScanString(db.Query(ctx, RawQuery("SELECT name FROM users WHERE email = 'test@gmail.com'")))
		require.NoError(t, err)
		require.Equal(t, "test", email)
	})

	t.Run("gaps", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply all migrations except #4
		runnerWithHoles, err := NewMigrationRunner(db, readerWithoutUpdates, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runnerWithHoles.ApplyAll(ctx))

		// Assert name column exists but is not yet populated
		namePtr, _, err := ScanNilString(db.Query(ctx, RawQuery("SELECT name FROM users WHERE email = 'test@gmail.com'")))
		require.NoError(t, err)
		require.Nil(t, namePtr)

		// Apply all missing migrations
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert name colum now populated
		name, _, err := ScanString(db.Query(ctx, RawQuery("SELECT name FROM users WHERE email = 'test@gmail.com'")))
		require.NoError(t, err)
		require.Equal(t, "test", name)
	})
}

func TestApplyCreateConcurrentIndex(t *testing.T) {
	definitions := []RawDefinition{
		{ID: 1, RawUpQuery: "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);"},
		{ID: 2, RawUpQuery: "INSERT INTO users (name, email) VALUES ('test1', 'test1@gmail.com');"},
		{ID: 3, RawUpQuery: "CREATE UNIQUE INDEX CONCURRENTLY users_email_idx ON users (email);"},
		{ID: 4, RawUpQuery: "INSERT INTO users (name, email) VALUES ('test2', 'test2@gmail.com');"},
	}
	reader := MigrationReaderFunc(func() ([]RawDefinition, error) { return definitions, nil })

	t.Run("CIC", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply all migrations from scratch
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert last migration (unique index) was applied
		err = db.Exec(ctx, Query(
			"INSERT INTO users (name, email) VALUES ({:name}, {:email})",
			Args{"name": "duplicate", "email": "test2@gmail.com"},
		))
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})

	t.Run("CIC (already created)", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply just the first migration
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.Apply(ctx, 2))

		// Create the index outside of the migration infrastructure
		require.NoError(t, db.Exec(ctx, RawQuery(definitions[2].RawUpQuery)))

		// Apply remaining migrations
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert last migration (unique index) was applied
		err = db.Exec(ctx, Query(
			"INSERT INTO users (name, email) VALUES ({:name}, {:email})",
			Args{"name": "duplicate", "email": "test2@gmail.com"},
		))
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})

	t.Run("CIC (invalid)", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply just the first migration
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.Apply(ctx, 2))

		// Create the index outside of the migration infrastructure and force it to be invalid
		require.NoError(t, db.Exec(ctx, RawQuery(definitions[2].RawUpQuery)))
		require.NoError(t, db.Exec(ctx, RawQuery(`
			UPDATE pg_index
			SET indisvalid = false
			WHERE indexrelid = (
				SELECT oid
				FROM pg_class
				WHERE relname = 'users_email_idx'
			);
		`)))

		// Apply remaining migrations
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert last migration (unique index) as applied
		err = db.Exec(ctx, Query(
			"INSERT INTO users (name, email) VALUES ({:name}, {:email})",
			Args{"name": "duplicate", "email": "test2@gmail.com"},
		))
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})

	t.Run("CIC (in progress)", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply the first two migrations
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.Apply(ctx, 2))

		var wg sync.WaitGroup
		errCh := make(chan error, 1)

		async := func(f func() error) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				if err := f(); err != nil {
					errCh <- err
				}
			}()
		}

		// Start a transaction and insert a row in the users table but don't
		// commit so that we hold open a lock for the following async tasks.
		tx, err := db.Transact(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Exec(ctx, RawQuery("INSERT INTO users (name, email) VALUES ('blocking', 'blocking@example.com')")))

		// Begin creating the index concurrently outside the migration runner
		// This will block until the transaction above commits or rolls back
		async(func() error { return db.Exec(ctx, RawQuery(definitions[2].RawUpQuery)) })

		// Jitter time to ensure index creation has started (and is blocked)
		<-time.After(1 * time.Second)

		// Apply the index creation and remaining migrations
		// This will block until the other index creation completes
		async(func() error { return runner.ApplyAll(ctx) })

		// Jitter time to ensure the ApplyAll has started (and is blocked)
		<-time.After(2 * time.Second)

		// Commit the transaction and allow the index creation to complete
		require.NoError(t, tx.Done(nil))

		// Sync on async tasks and check for errors
		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}

		// Assert that the migration runner has unblocked and the index exists
		err = db.Exec(ctx, Query(
			"INSERT INTO users (name, email) VALUES ({:name}, {:email})",
			Args{"name": "duplicate", "email": "test2@gmail.com"},
		))
		require.ErrorContains(t, err, "duplicate key value violates unique constraint")
	})
}

func TestUndo(t *testing.T) {
	definitions := []RawDefinition{
		{
			ID:           1,
			RawUpQuery:   "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);",
			RawDownQuery: "DROP TABLE users;",
		},
		{
			ID:           2,
			RawUpQuery:   "CREATE TABLE comments (id SERIAL PRIMARY KEY, content TEXT NOT NULL, user_id INTEGER NOT NULL);",
			RawDownQuery: "DROP TABLE comments;",
		},
		{
			ID:           3,
			RawUpQuery:   "ALTER TABLE comments ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE;",
			RawDownQuery: "ALTER TABLE comments DROP COLUMN updated_at;",
		},
		{
			ID:           4,
			RawUpQuery:   "ALTER TABLE comments ADD COLUMN created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",
			RawDownQuery: "ALTER TABLE comments DROP COLUMN created_at;",
		},

		{ID: 5, RawUpQuery: "INSERT INTO users (email) VALUES ('test@gmail.com');"},
		{ID: 6, RawUpQuery: "INSERT INTO comments (content, user_id) VALUES ('test', 1);"},
		{ID: 7, RawUpQuery: "UPDATE comments SET updated_at = NOW();"},
	}
	definitionsWithoutCreatedAt := []RawDefinition{definitions[0], definitions[1], definitions[2], definitions[4], definitions[5], definitions[6]}
	reader := MigrationReaderFunc(func() ([]RawDefinition, error) { return definitions, nil })
	readerWithoutCreatedAt := MigrationReaderFunc(func() ([]RawDefinition, error) { return definitionsWithoutCreatedAt, nil })

	t.Run("tail", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply all migrations
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.ApplyAll(ctx))

		// Assert columns exist and are populated
		updatedAt, _, err := ScanNilTimestamp(db.Query(ctx, RawQuery("SELECT created_at FROM comments WHERE user_id = 1")))
		require.NoError(t, err)
		require.NotNil(t, updatedAt)

		// Undo migrations that added created_at/updated_at columns
		require.NoError(t, runner.Undo(ctx, 3))

		// Assert columns dropped
		_, _, err = ScanString(db.Query(ctx, RawQuery("SELECT updated_at FROM comments WHERE user_id = 1")))
		require.ErrorContains(t, err, "column \"updated_at\" does not exist")
	})

	t.Run("gaps", func(t *testing.T) {
		db := NewTestDB(t)
		ctx := context.Background()

		// Apply all migrations
		runner, err := NewMigrationRunner(db, reader, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runner.ApplyAll(ctx))

		// Undo migrations but skip #4
		runnerWithHoles, err := NewMigrationRunner(db, readerWithoutCreatedAt, log.NewNilLogger())
		require.NoError(t, err)
		require.NoError(t, runnerWithHoles.Undo(ctx, 3))

		// Assert created_at exists but updated_at does not
		_, _, err = ScanNilTimestamp(db.Query(ctx, RawQuery("SELECT created_at FROM comments WHERE user_id = 1")))
		require.NoError(t, err)
		_, _, err = ScanString(db.Query(ctx, RawQuery("SELECT updated_at FROM comments WHERE user_id = 1")))
		require.ErrorContains(t, err, "column \"updated_at\" does not exist")

		// Undo migrations including #4
		require.NoError(t, runner.Undo(ctx, 3))

		// Assert both columns dropped
		_, _, err = ScanString(db.Query(ctx, RawQuery("SELECT created_at FROM comments WHERE user_id = 1")))
		require.ErrorContains(t, err, "column \"created_at\" does not exist")
	})
}
