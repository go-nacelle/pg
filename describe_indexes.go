package pgutil

import (
	"context"
	"fmt"
)

type IndexDescription struct {
	Name                 string
	IsPrimaryKey         bool
	IsUnique             bool
	IsExclusion          bool
	IsDeferrable         bool
	IndexDefinition      string
	ConstraintType       string
	ConstraintDefinition string
}

func (d IndexDescription) Equals(other IndexDescription) bool {
	return true &&
		d.Name == other.Name &&
		d.IsPrimaryKey == other.IsPrimaryKey &&
		d.IsUnique == other.IsUnique &&
		d.IsExclusion == other.IsExclusion &&
		d.IsDeferrable == other.IsDeferrable &&
		d.IndexDefinition == other.IndexDefinition &&
		d.ConstraintType == other.ConstraintType &&
		d.ConstraintDefinition == other.ConstraintDefinition

}

type index struct {
	Namespace            string
	TableName            string
	Name                 string
	IsPrimaryKey         bool
	IsUnique             bool
	IsExclusion          *bool
	IsDeferrable         *bool
	IndexDefinition      string
	ConstraintType       *string
	ConstraintDefinition *string
}

var scanIndexes = NewSliceScanner(func(s Scanner) (i index, _ error) {
	var (
		isPrimaryKey string
		isUnique     string
	)

	err := s.Scan(
		&i.Namespace,
		&i.TableName,
		&i.Name,
		&isPrimaryKey,
		&isUnique,
		&i.IsExclusion,
		&i.IsDeferrable,
		&i.IndexDefinition,
		&i.ConstraintType,
		&i.ConstraintDefinition,
	)

	i.IsPrimaryKey = truthy(isPrimaryKey)
	i.IsUnique = truthy(isUnique)
	return i, err
})

func describeIndexes(ctx context.Context, db DB) (map[string][]IndexDescription, error) {
	indexes, err := scanIndexes(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			table_class.relname AS table_name,
			index_class.relname AS name,
			i.indisprimary AS is_primary_key,
			i.indisunique AS is_unique,
			i.indisexclusion AS is_exclusion,
			con.condeferrable AS is_deferrable,
			pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS index_definition,
			con.contype AS constraint_type,
			pg_catalog.pg_get_constraintdef(con.oid, true) AS constraint_definition
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class table_class ON table_class.oid = i.indrelid
		JOIN pg_catalog.pg_class index_class ON index_class.oid = i.indexrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = table_class.relnamespace
		LEFT OUTER JOIN pg_catalog.pg_constraint con ON
			con.conrelid = i.indrelid AND
			con.conindid = i.indexrelid AND
			con.contype IN ('p', 'u', 'x')
		WHERE
			n.nspname NOT LIKE 'pg_%' AND
			n.nspname != 'information_schema'
		ORDER BY n.nspname, table_class.relname, index_class.relname
	`)))
	if err != nil {
		return nil, err
	}

	indexMap := map[string][]IndexDescription{}
	for _, index := range indexes {
		key := fmt.Sprintf("%q.%q", index.Namespace, index.TableName)

		indexMap[key] = append(indexMap[key], IndexDescription{
			Name:                 index.Name,
			IsPrimaryKey:         index.IsPrimaryKey,
			IsUnique:             index.IsUnique,
			IsExclusion:          deref(index.IsExclusion),
			IsDeferrable:         deref(index.IsDeferrable),
			IndexDefinition:      index.IndexDefinition,
			ConstraintType:       deref(index.ConstraintType),
			ConstraintDefinition: deref(index.ConstraintDefinition),
		})
	}

	return indexMap, nil
}
