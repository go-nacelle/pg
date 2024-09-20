package pgutil

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/go-nacelle/nacelle/v2"
)

type queryWrapper struct {
	db     sqlDB
	mu     *sync.Mutex
	logger nacelle.Logger
}

type sqlDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func newDBWrapper(db *sql.DB, logger nacelle.Logger) *queryWrapper {
	return &queryWrapper{
		db:     db,
		logger: logger,
	}
}

func newTxWrapper(tx *sql.Tx, logger nacelle.Logger) *queryWrapper {
	return &queryWrapper{
		db:     tx,
		mu:     new(sync.Mutex),
		logger: logger,
	}
}

func (db *queryWrapper) Query(ctx context.Context, q Q) (*sql.Rows, error) {
	start := time.Now()
	db.lock()
	defer db.unlock()

	query, args := q.Format()
	rows, err := db.db.QueryContext(ctx, query, args...)
	logQuery(db.logger, time.Since(start), err, query, args)
	return rows, err
}

func (db *queryWrapper) Exec(ctx context.Context, q Q) error {
	start := time.Now()
	db.lock()
	defer db.unlock()

	query, args := q.Format()
	_, err := db.db.ExecContext(ctx, query, args...)
	logQuery(db.logger, time.Since(start), err, query, args)
	return err
}

func (db *queryWrapper) lock() {
	if db.mu == nil {
		return
	}

	if !db.mu.TryLock() {
		start := time.Now()
		db.mu.Lock()
		logLockWait(db.logger, time.Since(start))
	}
}

func (db *queryWrapper) unlock() {
	if db.mu == nil {
		return
	}

	db.mu.Unlock()
}

func logQuery(logger nacelle.Logger, duration time.Duration, err error, query string, args []any) {
	fields := nacelle.LogFields{
		"query":    query,
		"args":     args,
		"err":      err,
		"duration": duration,
	}

	logger.DebugWithFields(fields, "sql query executed")
}

func logLockWait(logger nacelle.Logger, duration time.Duration) {
	fields := nacelle.LogFields{
		"duration": duration,
	}

	logger.WarningWithFields(fields, "transaction used concurrently")
}
