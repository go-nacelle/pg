package pgutil

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

var (
	ErrDoesNotExist  = fmt.Errorf("record does not exist")
	ErrAlreadyExists = fmt.Errorf("record already exists")

	postgresErrorMap = map[string]error{
		"foreign_key_violation": ErrDoesNotExist,
		"unique_violation":      ErrAlreadyExists,
	}
)

func HandleError(err error, description string) error {
	if err == sql.ErrNoRows {
		return ErrDoesNotExist
	}

	if postgresErr, ok := err.(*pq.Error); ok {
		if err, ok := postgresErrorMap[postgresErr.Code.Name()]; ok {
			return err
		}
	}

	return fmt.Errorf("%s (%s)", description, err.Error())
}
