package pgutil

import (
	"context"
	"fmt"
)

type ConstraintDescription struct {
	Name                string
	Type                string
	IsDeferrable        bool
	ReferencedTableName string
	Definition          string
}

func (d ConstraintDescription) Equals(other ConstraintDescription) bool {
	return true &&
		d.Name == other.Name &&
		d.Type == other.Type &&
		d.IsDeferrable == other.IsDeferrable &&
		d.ReferencedTableName == other.ReferencedTableName &&
		d.Definition == other.Definition
}

type constraint struct {
	Namespace           string
	TableName           string
	Name                string
	Type                string
	IsDeferrable        *bool
	ReferencedTableName *string
	Definition          string
}

var scanConstraints = NewSliceScanner(func(s Scanner) (c constraint, _ error) {
	err := s.Scan(
		&c.Namespace,
		&c.TableName,
		&c.Name,
		&c.Type,
		&c.IsDeferrable,
		&c.ReferencedTableName,
		&c.Definition,
	)
	return c, err
})

func describeConstraints(ctx context.Context, db DB) (map[string][]ConstraintDescription, error) {
	constraints, err := scanConstraints(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			table_class.relname AS table_name,
			con.conname AS name,
			con.contype AS type,
			con.condeferrable AS is_deferrable,
			reftable_class.relname AS ref_table_name,
			pg_catalog.pg_get_constraintdef(con.oid, true) AS definition
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class table_class ON table_class.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = table_class.relnamespace
		LEFT OUTER JOIN pg_catalog.pg_class reftable_class ON reftable_class.oid = con.confrelid
		WHERE
			n.nspname NOT LIKE 'pg_%' AND
			n.nspname != 'information_schema' AND
			con.contype IN ('c', 'f', 't')
		ORDER BY
			n.nspname,
			table_class.relname,
			con.conname
	`)))
	if err != nil {
		return nil, err
	}

	constraintMap := map[string][]ConstraintDescription{}
	for _, constraint := range constraints {
		key := fmt.Sprintf("%q.%q", constraint.Namespace, constraint.TableName)

		constraintMap[key] = append(constraintMap[key], ConstraintDescription{
			Name:                constraint.Name,
			Type:                constraint.Type,
			IsDeferrable:        deref(constraint.IsDeferrable),
			ReferencedTableName: deref(constraint.ReferencedTableName),
			Definition:          constraint.Definition,
		})
	}

	return constraintMap, nil
}
