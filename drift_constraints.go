package pgutil

import "fmt"

type ConstraintModifier struct {
	t TableDescription
	d ConstraintDescription
}

func NewConstraintModifier(_ SchemaDescription, t TableDescription, d ConstraintDescription) ConstraintModifier {
	return ConstraintModifier{
		t: t,
		d: d,
	}
}

func (m ConstraintModifier) Key() string {
	return fmt.Sprintf("%q.%q.%q", m.t.Namespace, m.t.Name, m.d.Name)
}

func (m ConstraintModifier) ObjectType() string {
	return "constraint"
}

func (m ConstraintModifier) Description() ConstraintDescription {
	return m.d
}

func (m ConstraintModifier) Create() string {
	return fmt.Sprintf("ALTER TABLE %q.%q ADD CONSTRAINT %q %s;", m.t.Namespace, m.t.Name, m.d.Name, m.d.Definition)
}

func (m ConstraintModifier) Drop() string {
	return fmt.Sprintf("ALTER TABLE %q.%q DROP CONSTRAINT IF EXISTS %q;", m.t.Namespace, m.t.Name, m.d.Name)
}
