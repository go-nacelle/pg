package database

import (
	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil"
)

func Dial(databaseURL string, logger log.Logger) (pgutil.DB, error) {
	return pgutil.Dial(databaseURL, logger)
}
