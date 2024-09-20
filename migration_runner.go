package pgutil

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/nacelle/v2"
	"github.com/jackc/pgconn"
)

type Runner struct {
	db          DB
	logger      nacelle.Logger
	definitions []Definition
	locker      *TransactionalLocker
}

func NewMigrationRunner(db DB, reader MigrationReader, logger nacelle.Logger) (*Runner, error) {
	definitions, err := ReadMigrations(reader)
	if err != nil {
		return nil, err
	}

	locker, err := NewTransactionalLocker(db, StringKey("nacelle/pgutil.migration-runner"))
	if err != nil {
		return nil, err
	}

	return &Runner{
		db:          db,
		logger:      logger,
		definitions: definitions,
		locker:      locker,
	}, nil
}

func (r *Runner) Definitions() []Definition {
	return r.definitions
}

func (r *Runner) ApplyAll(ctx context.Context) error {
	return r.apply(ctx, r.definitions)
}

func (r *Runner) Apply(ctx context.Context, id int) error {
	for i, definition := range r.definitions {
		if definition.ID == id {
			return r.apply(ctx, r.definitions[:i+1])
		}
	}

	return errors.New("migration not found")
}

func (r *Runner) apply(ctx context.Context, definitions []Definition) error {
	if err := r.ensureMigrationLogsTable(ctx); err != nil {
		return err
	}

	for {
		upToDate, cicDefinition, err := r.applyDefinitions(ctx, definitions, false)
		if err != nil || upToDate {
			return err
		}

		if cicDefinition != nil {
			if err := r.applyConcurrentIndexCreation(ctx, *cicDefinition); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) Undo(ctx context.Context, id int) error {
	if err := r.ensureMigrationLogsTable(ctx); err != nil {
		return err
	}

	for i, definition := range r.definitions {
		if definition.ID == id {
			definitions := slices.Clone(r.definitions[i:])
			slices.Reverse(definitions)

			// NOTE: CIC are illegal in down migrations; perform this in one shot
			_, _, err := r.applyDefinitions(ctx, definitions, true)
			return err
		}
	}

	return errors.New("migration not found")
}

func (r *Runner) ensureMigrationLogsTable(ctx context.Context) error {
	for _, query := range []string{
		"CREATE TABLE IF NOT EXISTS migration_logs(id SERIAL PRIMARY KEY)",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS migration_id integer NOT NULL",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS reverse bool NOT NULL",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL DEFAULT current_timestamp",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS last_heartbeat_at timestamptz",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS finished_at timestamptz",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS success boolean",
		"ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS error_message text",
	} {
		if err := r.db.Exec(ctx, RawQuery(query)); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) applyDefinitions(ctx context.Context, definitions []Definition, reverse bool) (upToDate bool, cicDefinition *Definition, _ error) {
	err := r.locker.WithLock(ctx, StringKey("ddl"), func(_ DB) (err error) {
		migrationLogs, err := r.MigrationLogs(ctx)
		if err != nil {
			return err
		}

		applied := map[int]struct{}{}
		for _, log := range migrationLogs {
			if log.Success != nil && *log.Success && !log.Reverse {
				applied[log.MigrationID] = struct{}{}
			}
		}

		var migrationsToApply []Definition
		for _, definition := range definitions {
			if _, ok := applied[definition.ID]; ok == reverse {
				migrationsToApply = append(migrationsToApply, definition)
			}
		}

		if len(migrationsToApply) == 0 {
			r.logger.Info("Migrations are in expected state")
			upToDate = true
			return nil
		}

		for _, definition := range migrationsToApply {
			if definition.IndexMetadata != nil && !reverse {
				// We can't perform CIC while holding a lock or else we'll deadlock.
				// Capture this definition to be applied outside of the lock we're holding.
				// We can skip this check for reverse application as CIC are illegal in down migrations.
				cicDefinition = &definition
				return nil
			}

			if err := r.withMigrationLog(ctx, definition, reverse, func(_ int) error {
				query, direction := definition.UpQuery, "up"
				if reverse {
					query, direction = definition.DownQuery, "down"
				}

				logger := r.logger.WithFields(log.LogFields{
					"id":        definition.ID,
					"name":      definition.Name,
					"direction": direction,
				})
				logger.Info("Applying migration")

				if err := r.db.WithTransaction(ctx, func(tx DB) error { return tx.Exec(ctx, query) }); err != nil {
					logger.ErrorWithFields(log.LogFields{"error": err}, "Failed to apply migration")
					return err
				}

				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})

	return upToDate, cicDefinition, err
}

func (r *Runner) applyConcurrentIndexCreation(ctx context.Context, definition Definition) error {
	tableName := definition.IndexMetadata.TableName
	indexName := definition.IndexMetadata.IndexName

	logger := r.logger.WithFields(log.LogFields{
		"id":        definition.ID,
		"name":      definition.Name,
		"direction": "up",
		"tableName": tableName,
		"indexName": indexName,
	})
	logger.Info("Handling concurrent index creation")

indexPollLoop:
	for i := 0; ; i++ {
		if i != 0 {
			if err := wait(ctx, time.Second*5); err != nil {
				return err
			}
		}

		indexStatus, exists, err := r.getIndexStatus(ctx, tableName, indexName)
		if err != nil {
			return err
		}

		if exists {
			logger.InfoWithFields(log.LogFields{
				"phase":        deref(indexStatus.Phase),
				"lockersTotal": deref(indexStatus.LockersTotal),
				"lockersDone":  deref(indexStatus.LockersDone),
				"blocksTotal":  deref(indexStatus.BlocksTotal),
				"blocksDone":   deref(indexStatus.BlocksDone),
				"tuplesTotal":  deref(indexStatus.TuplesTotal),
				"tuplesDone":   deref(indexStatus.TuplesDone),
			}, "Index exists")

			if indexStatus.IsValid {
				logger.Info("Index is valid")

				if recheck, err := r.handleValidIndex(ctx, definition); err != nil {
					return err
				} else if recheck {
					continue indexPollLoop
				} else {
					return nil
				}
			}

			if indexStatus.Phase != nil {
				continue indexPollLoop
			}

			logger.Info("Dropping invalid index")

			// NOTE: Must interpolate identifier here as placeholders aren't valid in this position.
			if err := r.db.Exec(ctx, queryf(`DROP INDEX IF EXISTS %s`, indexName)); err != nil {
				return err
			}
		}

		logger.Info("Creating index")

		if raceDetected, err := r.createIndexConcurrently(ctx, definition); err != nil {
			return err
		} else if raceDetected {
			continue indexPollLoop
		}

		return nil
	}
}

func (r *Runner) handleValidIndex(ctx context.Context, definition Definition) (recheck bool, _ error) {
	err := r.locker.WithLock(ctx, StringKey("log"), func(tx DB) error {
		log, ok, err := r.getLogForConcurrentIndex(ctx, tx, definition.ID)
		if err != nil {
			return err
		}
		if !ok {
			if err := tx.Exec(ctx, Query(`
				INSERT INTO migration_logs (migration_id, reverse, finished_at, success)
				VALUES ({:id}, false, current_timestamp, true)
			`,
				Args{"id": definition.ID},
			)); err != nil {
				return err
			}

			return nil
		}

		if log.Success != nil {
			if *log.Success {
				return nil
			}

			return errors.New(*log.ErrorMessage)
		}

		if time.Since(log.LastHeartbeatAt) >= time.Second*15 {
			recheck = true
			return nil
		}

		if err := tx.Exec(ctx, Query(`
			UPDATE migration_logs
			SET success = true, finished_at = current_timestamp
			WHERE id = {:id}
		`,
			Args{"id": log.ID},
		)); err != nil {
			return err
		}

		return nil
	})

	return recheck, err
}

func (r *Runner) createIndexConcurrently(ctx context.Context, definition Definition) (raceDetected bool, _ error) {
	err := r.withMigrationLog(ctx, definition, false, func(id int) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			for {
				if err := r.db.Exec(ctx, Query(`
					UPDATE migration_logs
					SET last_heartbeat_at = current_timestamp
					WHERE id = {:id}
				`,
					Args{"id": id},
				)); err != nil && ctx.Err() != context.Canceled {
					r.logger.ErrorWithFields(log.LogFields{
						"error": err,
					}, "Failed to update heartbeat")
				}

				if err := wait(ctx, time.Second*5); err != nil {
					break
				}
			}
		}()

		if err := r.db.Exec(ctx, definition.UpQuery); err != nil {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code == "42P07" {
				return err
			}

			if err := r.db.Exec(ctx, Query(
				`DELETE FROM migration_logs WHERE id = {:id}`,
				Args{"id": id},
			)); err != nil {
				return err
			}

			raceDetected = true
		}

		return nil
	})

	return raceDetected, err
}

//
//

type MigrationLog struct {
	MigrationID  int
	Reverse      bool
	Success      *bool
	ErrorMessage *string
}

var scanMigrationLogs = NewSliceScanner(func(s Scanner) (ms MigrationLog, _ error) {
	err := s.Scan(&ms.MigrationID, &ms.Reverse, &ms.Success, &ms.ErrorMessage)
	return ms, err
})

func (r *Runner) MigrationLogs(ctx context.Context) (map[int]MigrationLog, error) {
	if err := r.ensureMigrationLogsTable(ctx); err != nil {
		return nil, err
	}

	migrationLogs, err := scanMigrationLogs(r.db.Query(ctx, RawQuery(`
		WITH ranked_migration_logs AS (
			SELECT
				l.*,
				ROW_NUMBER() OVER (PARTITION BY migration_id ORDER BY started_at DESC) AS rank
			FROM migration_logs l
		)
		SELECT
			migration_id,
			reverse,
			success,
			error_message
		FROM ranked_migration_logs
		WHERE rank = 1
		ORDER BY migration_id
	`)))
	if err != nil {
		return nil, err
	}

	logMap := map[int]MigrationLog{}
	for _, state := range migrationLogs {
		logMap[state.MigrationID] = state
	}

	return logMap, nil
}

func (r *Runner) WriteMigrationLog(ctx context.Context, id int) error {
	if err := r.ensureMigrationLogsTable(ctx); err != nil {
		return err
	}

	var definition *Definition
	for _, def := range r.definitions {
		if def.ID == id {
			definition = &def
			break
		}
	}

	if definition == nil {
		return errors.New("migration not found")
	}

	return r.withMigrationLog(ctx, *definition, false, func(_ int) error {
		r.logger.InfoWithFields(log.LogFields{"id": id}, "Forcing writing migration log")
		return nil
	})
}

func (r *Runner) withMigrationLog(ctx context.Context, definition Definition, reverse bool, f func(id int) error) (err error) {
	id, _, err := ScanInt(r.db.Query(ctx, Query(`
		INSERT INTO migration_logs (migration_id, reverse)
		VALUES ({:id}, {:reverse})
		RETURNING id
	`, Args{
		"id":      definition.ID,
		"reverse": reverse,
	})))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, r.db.Exec(ctx, Query(`
			UPDATE migration_logs
			SET
				finished_at = current_timestamp,
				success = {:success},
				error_message = {:error_message}
			WHERE id = {:id}
		`, Args{
			"success":       err == nil,
			"error_message": extractErrorMessage(err),
			"id":            id,
		})))
	}()

	return f(id)
}

//
//

type indexStatus struct {
	IsValid      bool
	Phase        *string
	LockersTotal *int
	LockersDone  *int
	BlocksTotal  *int
	BlocksDone   *int
	TuplesTotal  *int
	TuplesDone   *int
}

var scanIndexStatus = NewFirstScanner(func(s Scanner) (is indexStatus, _ error) {
	err := s.Scan(
		&is.IsValid,
		&is.Phase,
		&is.LockersTotal,
		&is.LockersDone,
		&is.BlocksTotal,
		&is.BlocksDone,
		&is.TuplesTotal,
		&is.TuplesDone,
	)
	return is, err
})

func (r *Runner) getIndexStatus(ctx context.Context, tableName, indexName string) (indexStatus, bool, error) {
	return scanIndexStatus(r.db.Query(ctx, Query(`
		SELECT
			index.indisvalid,
			progress.phase,
			progress.lockers_total,
			progress.lockers_done,
			progress.blocks_total,
			progress.blocks_done,
			progress.tuples_total,
			progress.tuples_done
		FROM pg_catalog.pg_class table_class
		JOIN pg_catalog.pg_index index ON index.indrelid = table_class.oid
		JOIN pg_catalog.pg_class index_class ON index_class.oid = index.indexrelid
		LEFT JOIN pg_catalog.pg_stat_progress_create_index progress ON progress.relid = table_class.oid AND progress.index_relid = index_class.oid
		WHERE
			table_class.relname = {:tableName} AND
			index_class.relname = {:indexName}
	`, Args{
		"tableName": tableName,
		"indexName": indexName,
	})))
}

//
//

type concurrentIndexLog struct {
	ID              int
	Success         *bool
	ErrorMessage    *string
	LastHeartbeatAt time.Time
}

var scanConcurrentIndexLog = NewFirstScanner(func(s Scanner) (l concurrentIndexLog, _ error) {
	err := s.Scan(&l.ID, &l.Success, &l.ErrorMessage, &l.LastHeartbeatAt)
	return l, err
})

func (r *Runner) getLogForConcurrentIndex(ctx context.Context, db DB, id int) (concurrentIndexLog, bool, error) {
	return scanConcurrentIndexLog(db.Query(ctx, Query(`
		WITH ranked_migration_logs AS (
			SELECT
				l.*,
				ROW_NUMBER() OVER (ORDER BY started_at DESC) AS rank
			FROM migration_logs l
			WHERE migration_id = {:id}
		)
		SELECT
			id,
			success,
			error_message,
			COALESCE(last_heartbeat_at, started_at)
		FROM ranked_migration_logs
		WHERE rank = 1 AND NOT reverse
	`,
		Args{"id": id},
	)))
}

//
//

func wait(ctx context.Context, duration time.Duration) error {
	select {
	case <-time.After(duration):
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func extractErrorMessage(err error) *string {
	if err == nil {
		return nil
	}

	msg := err.Error()
	return &msg
}
