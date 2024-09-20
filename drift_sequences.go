package pgutil

import (
	"fmt"
	"strings"
)

type SequenceModifier struct {
	d SequenceDescription
}

func NewSequenceModifier(_ SchemaDescription, d SequenceDescription) SequenceModifier {
	return SequenceModifier{
		d: d,
	}
}

func (m SequenceModifier) Key() string {
	return fmt.Sprintf("%q.%q", m.d.Namespace, m.d.Name)
}

func (m SequenceModifier) ObjectType() string {
	return "sequence"
}

func (m SequenceModifier) Description() SequenceDescription {
	return m.d
}

func (m SequenceModifier) Create() string {
	minValue := "NO MINVALUE"
	if m.d.MinimumValue != 0 {
		minValue = fmt.Sprintf("MINVALUE %d", m.d.MinimumValue)
	}

	maxValue := "NO MAXVALUE"
	if m.d.MaximumValue != 0 {
		maxValue = fmt.Sprintf("MAXVALUE %d", m.d.MaximumValue)
	}

	return fmt.Sprintf(
		"CREATE SEQUENCE IF NOT EXISTS %s AS %s INCREMENT BY %d %s %s START WITH %d %s CYCLE;",
		m.Key(),
		m.d.Type,
		m.d.Increment,
		minValue,
		maxValue,
		m.d.StartValue,
		m.d.CycleOption,
	)
}

func (m SequenceModifier) Drop() string {
	return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s;", m.Key())
}

func (m SequenceModifier) AlterExisting(existingSchema SchemaDescription, existingObject SequenceDescription) ([]ddlStatement, bool) {
	parts := []string{
		fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s", m.Key()),
	}
	if m.d.Type != existingObject.Type {
		parts = append(parts, fmt.Sprintf("AS %s", m.d.Type))
	}
	if m.d.Increment != existingObject.Increment {
		parts = append(parts, fmt.Sprintf("INCREMENT BY %d", m.d.Increment))
	}
	if m.d.MinimumValue != existingObject.MinimumValue {
		parts = append(parts, fmt.Sprintf("MINVALUE %d", m.d.MinimumValue))
	}
	if m.d.MaximumValue != existingObject.MaximumValue {
		parts = append(parts, fmt.Sprintf("MAXVALUE %d", m.d.MaximumValue))
	}
	if m.d.StartValue != existingObject.StartValue {
		parts = append(parts, fmt.Sprintf("START WITH %d", m.d.StartValue))
	}
	if m.d.CycleOption != existingObject.CycleOption {
		if m.d.CycleOption == "YES" {
			parts = append(parts, "CYCLE")
		} else {
			parts = append(parts, "NO CYCLE")
		}
	}

	return []ddlStatement{
		newStatement(
			m.Key(),
			"replace",
			m.ObjectType(),
			strings.Join(parts, " ")+";",
		),
	}, true
}
