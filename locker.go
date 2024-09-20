package pgutil

import (
	"context"
	"errors"
	"math"

	"github.com/segmentio/fasthash/fnv1"
)

type TransactionalLocker struct {
	db        DB
	namespace int32
}

var ErrInTransaction = errors.New("locker database must not be in transaction")

func StringKey(key string) int32 {
	return int32(fnv1.HashString32(key) % math.MaxInt32)
}

func NewTransactionalLocker(db DB, namespace int32) (*TransactionalLocker, error) {
	if db.IsInTransaction() {
		return nil, ErrInTransaction
	}

	locker := &TransactionalLocker{
		db:        db,
		namespace: namespace,
	}

	return locker, nil
}

func (l *TransactionalLocker) WithLock(ctx context.Context, key int32, f func(tx DB) error) error {
	return l.db.WithTransaction(ctx, func(tx DB) error {
		if err := tx.Exec(ctx, Query("SELECT pg_advisory_xact_lock({:namespace}, {:key})", Args{
			"namespace": l.namespace,
			"key":       key,
		})); err != nil {
			return err
		}

		return f(tx)
	})
}

func (l *TransactionalLocker) TryWithLock(ctx context.Context, key int32, f func(tx DB) error) (acquired bool, _ error) {
	err := l.db.WithTransaction(ctx, func(tx DB) (err error) {
		if acquired, _, err = ScanBool(tx.Query(ctx, Query("SELECT pg_try_advisory_xact_lock({:namespace}, {:key})", Args{
			"namespace": l.namespace,
			"key":       key,
		}))); err != nil {
			return err
		} else if !acquired {
			return nil
		}

		return f(tx)
	})

	return acquired, err
}
