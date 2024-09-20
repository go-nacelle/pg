package pgutil

import (
	"context"
)

type ViewDescription struct {
	Namespace  string
	Name       string
	Definition string
}

func (d ViewDescription) Equals(other ViewDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name &&
		d.Definition == other.Definition
}

var scanViews = NewSliceScanner(func(s Scanner) (v ViewDescription, _ error) {
	err := s.Scan(&v.Namespace, &v.Name, &v.Definition)
	return v, err
})

func DescribeViews(ctx context.Context, db DB) ([]ViewDescription, error) {
	return scanViews(db.Query(ctx, RawQuery(`
		SELECT
			v.schemaname AS namespace,
			v.viewname AS name,
			v.definition AS definition
		FROM pg_catalog.pg_views v
		WHERE
			v.schemaname NOT LIKE 'pg_%' AND
			v.schemaname != 'information_schema'
		ORDER BY v.schemaname, v.viewname
	`)))
}
