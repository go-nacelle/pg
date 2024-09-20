package pgutil

import "fmt"

type TableModifier struct {
	d TableDescription
}

func NewTableModifier(_ SchemaDescription, d TableDescription) TableModifier {
	return TableModifier{
		d: d,
	}
}

func (m TableModifier) Key() string {
	return fmt.Sprintf("%q.%q", m.d.Namespace, m.d.Name)
}

func (m TableModifier) ObjectType() string {
	return "table"
}

func (m TableModifier) Description() TableDescription {
	return m.d
}

func (m TableModifier) Create() string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s();", m.Key())
}

func (m TableModifier) Drop() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", m.Key())
}

type TableComponentModifiers struct {
	Columns     []ColumnModifier
	Constraints []ConstraintModifier
	Indexes     []IndexModifier
}

func NewTableComponentModifiers(schema SchemaDescription, tables []TableDescription) TableComponentModifiers {
	var (
		columns     []ColumnModifier
		constraints []ConstraintModifier
		indexes     []IndexModifier
	)

	for _, table := range tables {
		columns = append(columns, wrapWithContextValues(schema, table, table.Columns, NewColumnModifier)...)
		constraints = append(constraints, wrapWithContextValues(schema, table, table.Constraints, NewConstraintModifier)...)
		indexes = append(indexes, wrapWithContextValues(schema, table, table.Indexes, NewIndexModifier)...)
	}

	return TableComponentModifiers{
		Columns:     columns,
		Constraints: constraints,
		Indexes:     indexes,
	}
}
