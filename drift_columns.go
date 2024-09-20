package pgutil

import (
	"fmt"
)

type ColumnModifier struct {
	t TableDescription
	d ColumnDescription
}

func NewColumnModifier(_ SchemaDescription, t TableDescription, d ColumnDescription) ColumnModifier {
	return ColumnModifier{
		t: t,
		d: d,
	}
}

func (m ColumnModifier) Key() string {
	return fmt.Sprintf("%q.%q.%q", m.t.Namespace, m.t.Name, m.d.Name)
}

func (m ColumnModifier) ObjectType() string {
	return "column"
}

func (m ColumnModifier) Description() ColumnDescription {
	return m.d
}

func (m ColumnModifier) Create() string {
	nullableExpr := ""
	if !m.d.IsNullable {
		nullableExpr = " NOT NULL"
	}

	defaultExpr := ""
	if m.d.Default != "" {
		defaultExpr = fmt.Sprintf(" DEFAULT %s", m.d.Default)
	}

	return fmt.Sprintf("ALTER TABLE %q.%q ADD COLUMN IF NOT EXISTS %q %s%s%s;", m.t.Namespace, m.t.Name, m.d.Name, m.d.Type, nullableExpr, defaultExpr)
}

func (m ColumnModifier) Drop() string {
	return fmt.Sprintf("ALTER TABLE %q.%q DROP COLUMN IF EXISTS %q;", m.t.Namespace, m.t.Name, m.d.Name)
}

func (m ColumnModifier) AlterExisting(existingSchema SchemaDescription, existingObject ColumnDescription) ([]ddlStatement, bool) {
	statements := []string{}
	alterColumn := func(format string, args ...any) {
		statements = append(statements, fmt.Sprintf(fmt.Sprintf("ALTER TABLE %q.%q ALTER COLUMN %q %s;", m.t.Namespace, m.t.Name, m.d.Name, format), args...))
	}

	if m.d.Type != existingObject.Type {
		alterColumn("SET DATA TYPE %s", m.d.Type)
	}
	if m.d.Default != existingObject.Default {
		if m.d.Default == "" {
			alterColumn("DROP DEFAULT")
		} else {
			alterColumn("SET DEFAULT %s", m.d.Default)
		}
	}
	if m.d.IsNullable != existingObject.IsNullable {
		if m.d.IsNullable {
			alterColumn("DROP NOT NULL")
		} else {
			alterColumn("SET NOT NULL")
		}
	}

	// TODO - handle CharacterMaximumLength
	// TODO - handle IsIdentity
	// TODO - handle IdentityGeneration
	// TODO - handle IsGenerated
	// TODO - handle GenerationExpression

	return []ddlStatement{
		newStatement(
			m.Key(),
			"replace",
			m.ObjectType(),
			statements...,
		),
	}, true
}
