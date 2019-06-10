package pgutil

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-nacelle/nacelle"
	"github.com/jmoiron/sqlx"
)

type (
	LoggingDB struct {
		*sqlx.DB
		logger nacelle.Logger
	}

	LoggingTx struct {
		*sqlx.Tx
		logger nacelle.Logger
	}
)

const MaxPingAttempts = 15

func Dial(url string, logger nacelle.Logger) (*LoggingDB, error) {
	db, err := sqlx.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database (%s)", err)
	}

	for attempts := 0; ; attempts++ {
		err := db.Ping()
		if err == nil {
			break
		}

		if attempts >= MaxPingAttempts {
			return nil, fmt.Errorf("failed to ping database within timeout")
		}

		logger.Error("Failed to ping database, will retry in 2s (%s)", err.Error())
		<-time.After(time.Second * 2)
	}

	return &LoggingDB{db, logger}, nil
}

func (db *LoggingDB) Beginx() (*LoggingTx, error) {
	tx, err := db.DB.Beginx()
	return &LoggingTx{tx, db.logger}, err
}

func (db *LoggingDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := db.DB.Query(query, args...)
	logQuery(db.logger, query, time.Since(start), args...)
	return rows, err
}

func (db *LoggingDB) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	start := time.Now()
	rows, err := db.DB.Queryx(query, args...)
	logQuery(db.logger, query, time.Since(start), args...)
	return rows, err
}

func (db *LoggingDB) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	start := time.Now()
	row := db.DB.QueryRowx(query, args...)
	logQuery(db.logger, query, time.Since(start), args...)
	return row
}

func (db *LoggingDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := db.DB.Exec(query, args...)
	logQuery(db.logger, query, time.Since(start), args...)
	return res, err
}

func (tx *LoggingTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := tx.Tx.Query(query, args...)
	logQuery(tx.logger, query, time.Since(start), args...)
	return rows, err
}

func (tx *LoggingTx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	start := time.Now()
	rows, err := tx.Tx.Queryx(query, args...)
	logQuery(tx.logger, query, time.Since(start), args...)
	return rows, err
}

func (tx *LoggingTx) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	start := time.Now()
	row := tx.Tx.QueryRowx(query, args...)
	logQuery(tx.logger, query, time.Since(start), args...)
	return row
}

func (tx *LoggingTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := tx.Tx.Exec(query, args...)
	logQuery(tx.logger, query, time.Since(start), args...)
	return res, err
}

func logQuery(logger nacelle.Logger, query string, duration time.Duration, args ...interface{}) {
	fields := nacelle.LogFields{
		"query":    query,
		"args":     args,
		"duration": duration,
	}

	logger.DebugWithFields(fields, "sql query executed")
}
