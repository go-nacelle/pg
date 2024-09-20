package pgutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-nacelle/log/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestTransaction(t *testing.T) {
	db := NewTestDB(t)
	setupTestTransactionTable(t, db)

	// Add record outside of transaction, ensure it's visible
	err := db.Exec(context.Background(), RawQuery(`INSERT INTO test (x, y) VALUES (1, 42)`))
	require.NoError(t, err)
	assert.Equal(t, map[int]int{1: 42}, testTableContents(t, db))

	// Add record inside of a transaction
	tx1, err := db.Transact(context.Background())
	require.NoError(t, err)
	err = tx1.Exec(context.Background(), RawQuery(`INSERT INTO test (x, y) VALUES (2, 43)`))
	require.NoError(t, err)

	// Add record inside of another transaction
	tx2, err := db.Transact(context.Background())
	require.NoError(t, err)
	err = tx2.Exec(context.Background(), RawQuery(`INSERT INTO test (x, y) VALUES (3, 44)`))
	require.NoError(t, err)

	// Check what's visible pre-commit/rollback
	assert.Equal(t, map[int]int{1: 42}, testTableContents(t, db))
	assert.Equal(t, map[int]int{1: 42, 2: 43}, testTableContents(t, tx1))
	assert.Equal(t, map[int]int{1: 42, 3: 44}, testTableContents(t, tx2))

	// Finalize transactions
	rollbackErr := errors.New("rollback")
	err = tx1.Done(rollbackErr)
	require.ErrorIs(t, err, rollbackErr)
	err = tx2.Done(nil)
	require.NoError(t, err)

	// Check what's visible post-commit/rollback
	assert.Equal(t, map[int]int{1: 42, 3: 44}, testTableContents(t, db))
}

func TestConcurrentTransactions(t *testing.T) {
	t.Run("creating transactions concurrently does not fail", func(t *testing.T) {
		shim := &captureShim{}
		db := NewTestDBWithLogger(t, log.FromMinimalLogger(shim))
		setupTestTransactionTable(t, db)

		var g errgroup.Group
		for i := 0; i < 10; i++ {
			routine := i

			g.Go(func() (err error) {
				tx, err := db.Transact(context.Background())
				if err != nil {
					return err
				}
				defer func() { err = tx.Done(err) }()

				if err := tx.Exec(context.Background(), RawQuery(`SELECT pg_sleep(0.1)`)); err != nil {
					return err
				}

				return tx.Exec(context.Background(), Query(
					`INSERT INTO test (x, y) VALUES ({:routine}, {:routine})`,
					Args{"routine": routine},
				))
			})
		}

		require.NoError(t, g.Wait())
		assert.NotContains(t, strings.Join(shim.logs, "\n"), "transaction used concurrently")
	})

	t.Run("parallel insertion on a single transaction does not fail but logs an error", func(t *testing.T) {
		shim := &captureShim{}
		db := NewTestDBWithLogger(t, log.FromMinimalLogger(shim))
		setupTestTransactionTable(t, db)

		tx, err := db.Transact(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := tx.Done(err); err != nil {
				require.NoError(t, err)
			}
		})

		var g errgroup.Group
		for i := 0; i < 10; i++ {
			routine := i
			g.Go(func() (err error) {
				if err := tx.Exec(context.Background(), RawQuery(`SELECT pg_sleep(0.1);`)); err != nil {
					return err
				}

				return tx.Exec(context.Background(), Query(
					`INSERT INTO test (x, y) VALUES ({:routine}, {:routine})`,
					Args{"routine": routine},
				))
			})
		}

		require.NoError(t, g.Wait())
		assert.Contains(t, strings.Join(shim.logs, "\n"), "transaction used concurrently")
	})
}

const numSavepointTests = 10

func TestSavepoints(t *testing.T) {
	for i := 0; i < numSavepointTests; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := NewTestDB(t)
			setupTestTransactionTable(t, db)

			// Make `n` nested transactions where the `i`th transaction is rolled back.
			// Test that all of the actions in any savepoint after this index is also rolled back.
			recurSavepoints(t, db, numSavepointTests, i)

			expected := map[int]int{}
			for j := numSavepointTests; j > i; j-- {
				expected[j] = j * 2
			}
			assert.Equal(t, expected, testTableContents(t, db))
		})
	}
}

func recurSavepoints(t *testing.T, db DB, index, rollbackAt int) {
	if index == 0 {
		return
	}

	tx, err := db.Transact(context.Background())
	require.NoError(t, err)
	defer func() {
		var doneErr error
		if index == rollbackAt {
			doneErr = errors.New("rollback")
		}

		err := tx.Done(doneErr)
		require.ErrorIs(t, err, doneErr)
	}()

	require.NoError(t, tx.Exec(context.Background(), Query(
		`INSERT INTO test (x, y) VALUES ({:index}, {:index} * 2)`,
		Args{"index": index},
	)))

	recurSavepoints(t, tx, index-1, rollbackAt)
}

func setupTestTransactionTable(t *testing.T, db DB) {
	require.NoError(t, db.Exec(context.Background(), RawQuery(`
		CREATE TABLE test (
			id SERIAL PRIMARY KEY,
			x  INTEGER NOT NULL,
			y  INTEGER NOT NULL
		);
	`)))
}

func testTableContents(t *testing.T, db DB) map[int]int {
	pairs, err := scanTestPairs(db.Query(context.Background(), RawQuery(`SELECT x, y FROM test`)))
	require.NoError(t, err)

	pairsMap := make(map[int]int)
	for _, p := range pairs {
		pairsMap[p.x] = p.y
	}

	return pairsMap
}

//
//

type captureShim struct {
	logs []string
}

func (n *captureShim) WithFields(log.LogFields) log.MinimalLogger {
	return n
}

func (n *captureShim) LogWithFields(level log.LogLevel, fields log.LogFields, format string, args ...interface{}) {
	n.logs = append(n.logs, fmt.Sprintf(format, args...))
}

func (n *captureShim) Sync() error {
	return nil
}
