package pgutil

import (
	"context"
	"fmt"
)

type ColumnDescription struct {
	Name                   string
	Type                   string
	IsNullable             bool
	Default                string
	CharacterMaximumLength int
	IsIdentity             bool
	IdentityGeneration     string
	IsGenerated            bool
	GenerationExpression   string
}

func (d ColumnDescription) Equals(other ColumnDescription) bool {
	return true &&
		d.Name == other.Name &&
		d.Type == other.Type &&
		d.IsNullable == other.IsNullable &&
		d.Default == other.Default &&
		d.CharacterMaximumLength == other.CharacterMaximumLength &&
		d.IsIdentity == other.IsIdentity &&
		d.IdentityGeneration == other.IdentityGeneration &&
		d.IsGenerated == other.IsGenerated &&
		d.GenerationExpression == other.GenerationExpression
}

type column struct {
	Namespace              string
	TableName              string
	Name                   string
	Type                   string
	IsNullable             bool
	Default                *string
	CharacterMaximumLength *int
	IsIdentity             bool
	IdentityGeneration     *string
	IsGenerated            bool
	GenerationExpression   *string
}

var scanColumns = NewSliceScanner(func(s Scanner) (c column, _ error) {
	var (
		isNullable  string
		isIdentity  string
		isGenerated string
	)

	err := s.Scan(
		&c.Namespace,
		&c.TableName,
		&c.Name,
		&c.Type,
		&isNullable,
		&c.Default,
		&c.CharacterMaximumLength,
		&isIdentity,
		&c.IdentityGeneration,
		&isGenerated,
		&c.GenerationExpression,
	)

	c.IsNullable = truthy(isNullable)
	c.IsIdentity = truthy(isIdentity)
	c.IsGenerated = truthy(isGenerated)
	return c, err
})

func describeColumns(ctx context.Context, db DB) (map[string][]ColumnDescription, error) {
	columns, err := scanColumns(db.Query(ctx, RawQuery(`
		SELECT
			c.table_schema AS namespace,
			c.table_name AS name,
			c.column_name AS column_name,
			CASE
				WHEN c.data_type = 'ARRAY' THEN COALESCE((
					SELECT e.data_type
					FROM information_schema.element_types e
					WHERE
						e.object_type = 'TABLE' AND
						e.object_catalog = c.table_catalog AND
						e.object_schema = c.table_schema AND
						e.object_name = c.table_name AND
						e.collection_type_identifier = c.dtd_identifier
				)) || '[]'
				WHEN c.data_type = 'USER-DEFINED'    THEN c.udt_name
				WHEN c.character_maximum_length != 0 THEN c.data_type || '(' || c.character_maximum_length::text || ')'
				ELSE c.data_type
			END AS type,
			c.is_nullable AS is_nullable,
			c.column_default AS default,
			c.character_maximum_length AS character_maximum_length,
			c.is_identity AS is_identity,
			c.identity_generation AS identity_generation,
			c.is_generated AS is_generated,
			c.generation_expression AS generation_expression
		FROM information_schema.columns c
		JOIN information_schema.tables t ON
			t.table_schema = c.table_schema AND
			t.table_name = c.table_name
		WHERE
			t.table_type = 'BASE TABLE' AND
			t.table_schema NOT LIKE 'pg_%' AND
			t.table_schema != 'information_schema'
		ORDER BY c.table_schema, c.table_name, c.column_name
	`)))
	if err != nil {
		return nil, err
	}

	columnMap := map[string][]ColumnDescription{}
	for _, column := range columns {
		key := fmt.Sprintf("%q.%q", column.Namespace, column.TableName)

		columnMap[key] = append(columnMap[key], ColumnDescription{
			Name:                   column.Name,
			Type:                   column.Type,
			IsNullable:             column.IsNullable,
			Default:                deref(column.Default),
			CharacterMaximumLength: deref(column.CharacterMaximumLength),
			IsIdentity:             column.IsIdentity,
			IdentityGeneration:     deref(column.IdentityGeneration),
			IsGenerated:            column.IsGenerated,
			GenerationExpression:   deref(column.GenerationExpression),
		})
	}

	return columnMap, nil
}
