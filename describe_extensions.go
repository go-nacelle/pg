package pgutil

import (
	"context"
)

type ExtensionDescription struct {
	Namespace string
	Name      string
}

func (d ExtensionDescription) Equals(other ExtensionDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name
}

var scanExtensions = NewSliceScanner(func(s Scanner) (e ExtensionDescription, _ error) {
	err := s.Scan(&e.Namespace, &e.Name)
	return e, err
})

func DescribeExtensions(ctx context.Context, db DB) ([]ExtensionDescription, error) {
	return scanExtensions(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			e.extname AS name
		FROM pg_catalog.pg_extension e
		JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
		WHERE
			n.nspname NOT LIKE 'pg_%' AND
			n.nspname != 'information_schema'
		ORDER BY n.nspname, e.extname
	`)))
}
