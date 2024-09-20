package pgutil

import "fmt"

type TriggerModifier struct {
	d TriggerDescription
}

func NewTriggerModifier(_ SchemaDescription, d TriggerDescription) TriggerModifier {
	return TriggerModifier{
		d: d,
	}
}

func (m TriggerModifier) Key() string {
	return fmt.Sprintf("%q.%q", m.d.Namespace, m.d.Name)
}

func (m TriggerModifier) ObjectType() string {
	return "trigger"
}

func (m TriggerModifier) Description() TriggerDescription {
	return m.d
}

func (m TriggerModifier) Create() string {
	return fmt.Sprintf("%s;", m.d.Definition)
}

func (m TriggerModifier) Drop() string {
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %q ON %q.%q;", m.d.Name, m.d.Namespace, m.d.TableName)
}
