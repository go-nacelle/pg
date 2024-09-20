package pgutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocker(t *testing.T) {
	var (
		db  = NewTestDB(t)
		ctx = context.Background()
	)

	locker, err := NewTransactionalLocker(db, StringKey("test"))
	require.NoError(t, err)

	t.Run("sequential", func(t *testing.T) {
		require.NoError(t, locker.WithLock(ctx, 125, func(tx DB) error {
			return nil
		}))

		require.NoError(t, locker.WithLock(ctx, 125, func(tx DB) error {
			return nil
		}))
	})

	t.Run("concurrent", func(t *testing.T) {
		runWithHeldLock := func(f func()) {
			var (
				signal = make(chan struct{}) // closed when key=125 is acquired
				block  = make(chan struct{}) // closed when key=125 should be released
				errors = make(chan error, 1) // holds acquisition error from goroutine
			)

			go func() {
				defer close(errors)

				if err := locker.WithLock(ctx, 125, func(tx DB) error {
					close(signal)
					<-block
					return nil
				}); err != nil {
					errors <- err
				}
			}()

			<-signal     // Wait for key=125 to be acquired by goroutine above
			f()          // Run test function with held lock
			close(block) // Unblock test routine

			for err := range errors {
				require.NoError(t, err)
			}
		}

		runWithHeldLock(func() {
			// Test acquisition of concurrently held lock
			acquired, err := locker.TryWithLock(ctx, 125, func(tx DB) error {
				return nil
			})
			require.NoError(t, err)
			assert.False(t, acquired)

			// Test acquisition of concurrently un-held lock
			acquired, err = locker.TryWithLock(ctx, 126, func(tx DB) error {
				return nil
			})
			require.NoError(t, err)
			assert.True(t, acquired)
		})

		// Test acquisition of released lock
		acquired, err := locker.TryWithLock(ctx, 125, func(tx DB) error {
			return nil
		})
		require.NoError(t, err)
		assert.True(t, acquired)
	})
}
