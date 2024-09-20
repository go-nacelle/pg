package pgutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-nacelle/nacelle/v2"
)

type loggingTx struct {
	*queryWrapper
	tx    *sql.Tx
	start time.Time
}

func (tx *loggingTx) WithTransaction(ctx context.Context, f func(tx DB) error) error {
	return withTransaction(ctx, tx, f)
}

func (tx *loggingTx) IsInTransaction() bool {
	return true
}

func (tx *loggingTx) Transact(ctx context.Context) (DB, error) {
	return createSavepoint(ctx, tx)
}

func (tx *loggingTx) Done(err error) (combinedErr error) {
	defer func() { logDone(tx.logger, time.Since(tx.start), combinedErr) }()

	if err != nil {
		rollbackErr := tx.tx.Rollback()
		return errors.Join(err, rollbackErr)
	}

	return tx.tx.Commit()
}

type loggingSavepoint struct {
	*loggingTx
	savepointID string
	start       time.Time
}

func createSavepoint(ctx context.Context, tx *loggingTx) (*loggingSavepoint, error) {
	start := time.Now()

	id, err := randomHexString(16)
	if err != nil {
		return nil, err
	}
	savepointID := fmt.Sprintf("sp_%s", id)

	// NOTE: Must interpolate identifier here as placeholders aren't valid in this position.
	if err := tx.Exec(ctx, queryf("SAVEPOINT %s", savepointID)); err != nil {
		return nil, err
	}

	return &loggingSavepoint{
		loggingTx:   tx,
		savepointID: savepointID,
		start:       start,
	}, nil
}

func (tx *loggingSavepoint) WithTransaction(ctx context.Context, f func(tx DB) error) error {
	return withTransaction(ctx, tx, f)
}

func (tx *loggingSavepoint) IsInTransaction() bool {
	return true
}

func (tx *loggingSavepoint) Transact(ctx context.Context) (DB, error) {
	return createSavepoint(ctx, tx.loggingTx)
}

func (tx *loggingSavepoint) Done(err error) (combinedErr error) {
	defer func() { logDone(tx.logger, time.Since(tx.start), combinedErr) }()

	if err != nil {
		// NOTE: Must interpolate identifier here as placeholders aren't valid in this position.
		return errors.Join(err, tx.Exec(context.Background(), queryf("ROLLBACK TO %s", tx.savepointID)))
	}

	// NOTE: Must interpolate identifier here as placeholders aren't valid in this position.
	return tx.Exec(context.Background(), queryf("RELEASE %s", tx.savepointID))
}

var ErrPanicDuringTransaction = fmt.Errorf("encountered panic during transaction")

func withTransaction(ctx context.Context, db DB, f func(tx DB) error) (err error) {
	tx, err := db.Transact(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = tx.Done(ErrPanicDuringTransaction)
			panic(r)
		}

		err = tx.Done(err)
	}()

	return f(tx)
}

func logDone(logger nacelle.Logger, duration time.Duration, err error) {
	fields := nacelle.LogFields{
		"err":      err,
		"duration": duration,
	}

	logger.DebugWithFields(fields, "transaction closed")
}
