package pgutil

import "fmt"

type IndexModifier struct {
	t TableDescription
	d IndexDescription
}

func NewIndexModifier(_ SchemaDescription, t TableDescription, d IndexDescription) IndexModifier {
	return IndexModifier{
		t: t,
		d: d,
	}
}

func (m IndexModifier) Key() string {
	return fmt.Sprintf("%q.%q.%q", m.t.Namespace, m.t.Name, m.d.Name)
}

func (m IndexModifier) ObjectType() string {
	return "index"
}

func (m IndexModifier) Description() IndexDescription {
	return m.d
}

func (m IndexModifier) Create() string {
	if m.isConstraint() {
		return fmt.Sprintf("ALTER TABLE %q.%q ADD CONSTRAINT %q %s;", m.t.Namespace, m.t.Name, m.d.Name, m.d.ConstraintDefinition)
	}

	return fmt.Sprintf("%s;", m.d.IndexDefinition)
}

func (m IndexModifier) Drop() string {
	if m.isConstraint() {
		return fmt.Sprintf("ALTER TABLE %q.%q DROP CONSTRAINT IF EXISTS %q;", m.t.Namespace, m.t.Name, m.d.Name)
	}

	return fmt.Sprintf("DROP INDEX IF EXISTS %q.%q;", m.t.Namespace, m.d.Name)
}

func (m IndexModifier) isConstraint() bool {
	return m.d.ConstraintType == "u" || m.d.ConstraintType == "p"
}
