package pgutil

import "context"

type EnumDependency struct {
	EnumNamespace  string
	EnumName       string
	TableNamespace string
	TableName      string
	ColumnName     string
}

var scanEnumDependencies = NewSliceScanner(func(s Scanner) (d EnumDependency, _ error) {
	err := s.Scan(
		&d.EnumNamespace,
		&d.EnumName,
		&d.TableNamespace,
		&d.TableName,
		&d.ColumnName,
	)
	return d, err
})

func DescribeEnumDependencies(ctx context.Context, db DB) ([]EnumDependency, error) {
	return scanEnumDependencies(db.Query(ctx, RawQuery(`
		SELECT
			ns.nspname AS enum_namespace,
			col.udt_name AS enum_name,
			col.table_schema AS table_namespace,
			col.table_name AS table_name,
			col.column_name AS column_name
		FROM information_schema.columns col
		JOIN information_schema.tables tab
		ON
			tab.table_schema = col.table_schema AND
			tab.table_name = col.table_name AND
			tab.table_type = 'BASE TABLE'
		JOIN pg_type typ ON col.udt_name = typ.typname
		JOIN pg_namespace ns ON ns.oid = typ.typnamespace
		WHERE
			col.table_schema NOT LIKE 'pg_%' AND
			col.table_schema != 'information_schema' AND
			typ.typtype = 'e'
		ORDER BY col.table_schema, col.table_name, col.ordinal_position
	`)))
}

type ColumnDependency struct {
	SourceNamespace       string
	SourceTableOrViewName string
	SourceColumnName      string
	UsedNamespace         string
	UsedTableOrView       string
}

var scanColumnDependencies = NewSliceScanner(func(s Scanner) (d ColumnDependency, _ error) {
	err := s.Scan(
		&d.SourceNamespace,
		&d.SourceTableOrViewName,
		&d.SourceColumnName,
		&d.UsedNamespace,
		&d.UsedTableOrView,
	)
	return d, err
})

func DescribeColumnDependencies(ctx context.Context, db DB) ([]ColumnDependency, error) {
	return scanColumnDependencies(db.Query(ctx, RawQuery(`
		SELECT
			source_ns.nspname AS source_namespace,
			source_table.relname AS source_table_or_view_name,
			pg_attribute.attname AS source_column_name,
			dependent_ns.nspname AS used_namespace,
			dependent_view.relname AS used_table_or_view_name
		FROM pg_depend
		JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
		JOIN pg_class AS dependent_view ON pg_rewrite.ev_class = dependent_view.oid
		JOIN pg_class AS source_table ON pg_depend.refobjid = source_table.oid
		JOIN pg_attribute ON
			pg_depend.refobjid = pg_attribute.attrelid AND
			pg_depend.refobjsubid = pg_attribute.attnum
		JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
		JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
		WHERE
			dependent_ns.nspname NOT LIKE 'pg_%' AND
			dependent_ns.nspname != 'information_schema' AND
			source_ns.nspname NOT LIKE 'pg_%' AND
			source_ns.nspname != 'information_schemea'
		ORDER BY dependent_ns.nspname, dependent_view.relname
	`)))
}
