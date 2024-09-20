package pgutil

import (
	"fmt"
	"strings"
)

type EnumModifier struct {
	s SchemaDescription
	d EnumDescription
}

func NewEnumModifier(s SchemaDescription, d EnumDescription) EnumModifier {
	return EnumModifier{
		s: s,
		d: d,
	}
}

func (m EnumModifier) Key() string {
	return fmt.Sprintf("%q.%q", m.d.Namespace, m.d.Name)
}

func (m EnumModifier) ObjectType() string {
	return "enum"
}

func (m EnumModifier) Description() EnumDescription {
	return m.d
}

func (m EnumModifier) Create() string {
	var quotedLabels []string
	for _, label := range m.d.Labels {
		quotedLabels = append(quotedLabels, enumQuote(label))
	}

	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);", m.Key(), strings.Join(quotedLabels, ", "))
}

func (m EnumModifier) Drop() string {
	return fmt.Sprintf("DROP TYPE IF EXISTS %s;", m.Key())
}

// NOTE: This depends on the order of the schema being modified. We must be certain that the order of
// drop/apply/create ensures that the columns here in the existing schema are still valid within the
// schema being altered.
func (m EnumModifier) AlterExisting(existingSchema SchemaDescription, existingObject EnumDescription) ([]ddlStatement, bool) {
	if reconstruction, ok := unifyLabels(m.d.Labels, existingObject.Labels); ok {
		return m.alterViaReconstruction(reconstruction)
	}

	return m.alterViaDropAndRecreate(existingSchema)
}

func (m EnumModifier) alterViaReconstruction(reconstruction []missingLabel) ([]ddlStatement, bool) {
	var statements []string
	for _, missingLabel := range reconstruction {
		relativeTo := ""
		if missingLabel.Next != nil {
			relativeTo = fmt.Sprintf("BEFORE %s", enumQuote(*missingLabel.Next))
		} else {
			relativeTo = fmt.Sprintf("AFTER %s", enumQuote(*missingLabel.Prev))
		}

		statements = append(statements, fmt.Sprintf("ALTER TYPE %q.%q ADD VALUE %s %s;", m.d.Namespace, m.d.Name, enumQuote(missingLabel.Label), relativeTo))
	}

	return []ddlStatement{
		newStatement(
			m.Key(),
			"replace",
			m.ObjectType(),
			statements...,
		),
	}, true
}

func (m EnumModifier) alterViaDropAndRecreate(existingSchema SchemaDescription) ([]ddlStatement, bool) {
	// Basic plan:
	//   1. Rename the existing enum type.
	//   2. Create the new enum type with the old name.
	//   3. Drop all views that depend (transitively) on the enum type.
	//   4. Drop defaults on columns that reference the enum type.
	//   5. Alter column types to reference the new enum type.
	//   6. Re-add any defaults that were dropped.
	//   7. Recreate the views that were dropped.
	//   8. Drop the old enum type.
	//
	// NOTE: View statements are ordered by `Compare`. We do, however, need to be cautious
	// of the order in which we modify the enums and tables.

	// Select the dependencies that are relevant to the enum type we're modifying.
	var dependencies []EnumDependency
	for _, dependency := range existingSchema.EnumDependencies {
		if dependency.EnumNamespace == m.d.Namespace && dependency.EnumName == m.d.Name {
			dependencies = append(dependencies, dependency)
		}
	}

	// Calculate the transitive dependencies for all views in the current schema.
	createDependencyClosure, _ := viewDependencyClosures(existingSchema, SchemaDescription{})

	// Collect the set of views referencing a table with a column of the enum type.
	var views []string
	for _, dependency := range dependencies {
		for key := range createDependencyClosure[fmt.Sprintf("%q.%q", dependency.TableNamespace, dependency.TableName)] {
			views = append(views, key)
		}
	}

	// Generate ALTER TABLE statements for each table with a column of the enum type.
	var alterTableStatements []string
	for _, dependency := range dependencies {
		defaultValue := getDefaultValue(
			existingSchema.Tables,
			dependency.EnumNamespace,
			dependency.TableName,
			dependency.ColumnName,
		)

		var alterTableActions []string
		if defaultValue != "" {
			alterTableActions = append(alterTableActions, fmt.Sprintf(
				"ALTER COLUMN %q DROP DEFAULT",
				dependency.ColumnName,
			))
		}

		alterTableActions = append(alterTableActions, fmt.Sprintf(
			"ALTER COLUMN %q TYPE %s USING (%q::text::%s)",
			dependency.ColumnName,
			m.Key(),
			dependency.ColumnName,
			m.Key(),
		))

		if defaultValue != "" {
			alterTableActions = append(alterTableActions, fmt.Sprintf(
				"ALTER COLUMN %q SET DEFAULT %s",
				dependency.ColumnName,
				defaultValue,
			))
		}

		alterTableStatements = append(alterTableStatements, fmt.Sprintf(
			"ALTER TABLE %q.%q %s;",
			dependency.TableNamespace,
			dependency.TableName,
			strings.Join(alterTableActions, ", "),
		))
	}

	// Generate DROP/CREATE VIEW statements for each view that references the enum type.
	var viewStatements []ddlStatement
	for _, viewKey := range views {
		viewStatements = append(viewStatements, newStatement(
			viewKey,
			"drop",
			"view",
			fmt.Sprintf("DROP VIEW IF EXISTS %s;", viewKey),
		))

		// Look for the view in the new schema (which may have been dropped or modified). If the view
		// exists and has the SAME definition, then we need to be sure to issue a recreation statement,
		// otherwise we've dropped the view as an unintentional side-effect. If the view exists and has
		// a different definition, then we don't need to recreate the view because it will be recreated
		// as part of the normal view drift repair.

		var existingDefinition string
		for _, view := range existingSchema.Views {
			if fmt.Sprintf("%q.%q", view.Namespace, view.Name) == viewKey {
				existingDefinition = view.Definition
			}
		}

		for _, view := range m.s.Views {
			if viewKey == fmt.Sprintf("%q.%q", view.Namespace, view.Name) && view.Definition == existingDefinition {
				viewStatements = append(viewStatements, newStatement(
					viewKey,
					"create",
					"view",
					fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", viewKey, strings.TrimSpace(stripIdent(" "+existingDefinition))),
				))
			}
		}
	}

	// Construct enum replacement statements.
	var enumStatements []string
	enumStatements = append(enumStatements, fmt.Sprintf("ALTER TYPE %q.%q RENAME TO %q;", m.d.Namespace, m.d.Name, m.d.Name+"_bak"))
	enumStatements = append(enumStatements, m.Create())
	enumStatements = append(enumStatements, alterTableStatements...)
	enumStatements = append(enumStatements, fmt.Sprintf("DROP TYPE %q.%q;", m.d.Namespace, m.d.Name+"_bak"))

	return append(viewStatements, newStatement(
		m.Key(),
		"replace",
		m.ObjectType(),
		enumStatements...,
	)), true
}

func enumQuote(label string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(label, "'", "''"))
}

func getDefaultValue(tables []TableDescription, namespace, tableName, columnName string) string {
	for _, table := range tables {
		if table.Namespace == namespace && table.Name == tableName {
			for _, c := range table.Columns {
				if c.Name == columnName {
					return c.Default
				}
			}
		}
	}

	return ""
}

type missingLabel struct {
	Label string
	Prev  *string
	Next  *string
}

func unifyLabels(expectedLabels, existingLabels []string) (reconstruction []missingLabel, _ bool) {
	var (
		j               = 0
		missingIndexMap = map[int]struct{}{}
	)

	for i, label := range expectedLabels {
		if j < len(existingLabels) && existingLabels[j] == label {
			j++
		} else if i > 0 {
			missingIndexMap[i] = struct{}{}
		}
	}

	if j < len(existingLabels) {
		return nil, false
	}

	if expectedLabels[0] != existingLabels[0] {
		reconstruction = append(reconstruction, missingLabel{
			Label: expectedLabels[0],
			Next:  &existingLabels[0],
		})
	}

	for i, label := range expectedLabels {
		if _, ok := missingIndexMap[i]; ok {
			reconstruction = append(reconstruction, missingLabel{
				Label: label,
				Prev:  &expectedLabels[i-1],
			})
		}
	}

	return reconstruction, true
}
