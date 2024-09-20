package pgutil

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-nacelle/nacelle/v2"
)

const MaxPingAttempts = 15

func Dial(url string, logger nacelle.Logger) (DB, error) {
	db, err := sql.Open("postgres", url)
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

	return newLoggingDB(db, logger), nil
}
