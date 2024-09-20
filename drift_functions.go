package pgutil

import (
	"fmt"
	"strings"
)

type FunctionModifier struct {
	d FunctionDescription
}

func NewFunctionModifier(_ SchemaDescription, d FunctionDescription) FunctionModifier {
	return FunctionModifier{
		d: d,
	}
}

func (m FunctionModifier) Key() string {
	return fmt.Sprintf("%q.%q(%s)", m.d.Namespace, m.d.Name, strings.Join(m.d.ArgTypes, ", "))
}

func (m FunctionModifier) ObjectType() string {
	return "function"
}

func (m FunctionModifier) Description() FunctionDescription {
	return m.d
}

func (m FunctionModifier) Create() string {
	return fmt.Sprintf("%s;", m.d.Definition)
}

func (m FunctionModifier) Drop() string {
	return fmt.Sprintf("DROP FUNCTION IF EXISTS %s;", m.Key())
}

func (m FunctionModifier) AlterExisting(_ SchemaDescription, _ FunctionDescription) ([]ddlStatement, bool) {
	return []ddlStatement{
		newStatement(
			m.Key(),
			"create",
			m.ObjectType(),
			m.Create(),
		),
	}, true
}
