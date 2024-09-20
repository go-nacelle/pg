package pgutil

import (
	"context"
	"fmt"
	"strings"
)

type TableDescription struct {
	Namespace   string
	Name        string
	Columns     []ColumnDescription
	Constraints []ConstraintDescription
	Indexes     []IndexDescription
}

// Note: not a deep comparison
func (d TableDescription) Equals(other TableDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name
}

type table struct {
	Namespace string
	Name      string
}

var scanTables = NewSliceScanner(func(s Scanner) (t table, _ error) {
	err := s.Scan(&t.Namespace, &t.Name)
	return t, err
})

func DescribeTables(ctx context.Context, db DB) ([]TableDescription, error) {
	tables, err := scanTables(db.Query(ctx, RawQuery(`
		SELECT
			t.table_schema AS namespace,
			t.table_name AS name
		FROM information_schema.tables t
		WHERE
			t.table_type = 'BASE TABLE' AND
			t.table_schema NOT LIKE 'pg_%' AND
			t.table_schema != 'information_schema'
		ORDER BY t.table_schema, t.table_name
	`)))
	if err != nil {
		return nil, err
	}

	columnMap, err := describeColumns(ctx, db)
	if err != nil {
		return nil, err
	}

	constraintMap, err := describeConstraints(ctx, db)
	if err != nil {
		return nil, err
	}

	indexMap, err := describeIndexes(ctx, db)
	if err != nil {
		return nil, err
	}

	var hydratedTables []TableDescription
	for _, table := range tables {
		key := fmt.Sprintf("%q.%q", table.Namespace, table.Name)

		hydratedTables = append(hydratedTables, TableDescription{
			Namespace:   table.Namespace,
			Name:        table.Name,
			Columns:     columnMap[key],
			Constraints: constraintMap[key],
			Indexes:     indexMap[key],
		})
	}

	return hydratedTables, nil
}

//
//

func truthy(value string) bool {
	// truthy strings + SQL spec YES_NO
	return strings.ToLower(value) == "yes" || strings.ToLower(value) == "true"
}

func deref[T any](p *T) (v T) {
	if p != nil {
		v = *p
	}

	return
}
