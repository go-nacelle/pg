package pgutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-nacelle/nacelle/v2"
)

type DB interface {
	Query(ctx context.Context, query Q) (*sql.Rows, error)
	Exec(ctx context.Context, query Q) error
	WithTransaction(ctx context.Context, f func(tx DB) error) error

	IsInTransaction() bool
	Transact(ctx context.Context) (DB, error)
	Done(err error) error
}

type loggingDB struct {
	*queryWrapper
	db *sql.DB
}

func newLoggingDB(db *sql.DB, logger nacelle.Logger) *loggingDB {
	return &loggingDB{
		queryWrapper: newDBWrapper(db, logger),
		db:           db,
	}
}

func (db *loggingDB) WithTransaction(ctx context.Context, f func(tx DB) error) error {
	return withTransaction(ctx, db, f)
}

func (db *loggingDB) IsInTransaction() bool {
	return false
}

func (db *loggingDB) Transact(ctx context.Context) (DB, error) {
	start := time.Now()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &loggingTx{
		queryWrapper: newTxWrapper(tx, db.logger),
		tx:           tx,
		start:        start,
	}, nil
}

var ErrNotInTransaction = fmt.Errorf("not in a transaction")

func (db *loggingDB) Done(err error) error {
	return errors.Join(err, ErrNotInTransaction)
}
