package pgutil

import (
	"fmt"
	"strings"
)

type ViewModifier struct {
	d ViewDescription
}

func NewViewModifier(_ SchemaDescription, d ViewDescription) ViewModifier {
	return ViewModifier{
		d: d,
	}
}

func (m ViewModifier) Key() string {
	return fmt.Sprintf("%q.%q", m.d.Namespace, m.d.Name)
}

func (m ViewModifier) ObjectType() string {
	return "view"
}

func (m ViewModifier) Description() ViewDescription {
	return m.d
}

func (m ViewModifier) Create() string {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", m.Key(), strings.TrimSpace(stripIdent(" "+m.d.Definition)))
}

func (m ViewModifier) Drop() string {
	return fmt.Sprintf("DROP VIEW IF EXISTS %s;", m.Key())
}

func stripIdent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")

	min := len(lines[0])
	for _, line := range lines {
		if ident := len(line) - len(strings.TrimLeft(line, " ")); ident < min {
			min = ident
		}
	}

	for i, line := range lines {
		lines[i] = line[min:]
	}

	return strings.Join(lines, "\n")
}
