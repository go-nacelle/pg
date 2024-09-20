package pgutil

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
)

func Compare(a, b SchemaDescription) (statements []string) {
	var (
		aTableComponentModifiers = NewTableComponentModifiers(a, a.Tables)
		bTableComponentModifiers = NewTableComponentModifiers(b, b.Tables)
	)

	uniqueStatements := map[string]ddlStatement{}
	for _, statements := range [][]ddlStatement{
		compareObjects(a, b, wrapWithContextValue(a, a.Extensions, NewExtensionModifier), wrapWithContextValue(b, b.Extensions, NewExtensionModifier)),
		compareObjects(a, b, wrapWithContextValue(a, a.Enums, NewEnumModifier), wrapWithContextValue(b, b.Enums, NewEnumModifier)),
		compareObjects(a, b, wrapWithContextValue(a, a.Functions, NewFunctionModifier), wrapWithContextValue(b, b.Functions, NewFunctionModifier)),
		compareObjects(a, b, wrapWithContextValue(a, a.Tables, NewTableModifier), wrapWithContextValue(b, b.Tables, NewTableModifier)),
		compareObjects(a, b, wrapWithContextValue(a, a.Sequences, NewSequenceModifier), wrapWithContextValue(b, b.Sequences, NewSequenceModifier)),
		compareObjects(a, b, aTableComponentModifiers.Columns, bTableComponentModifiers.Columns),
		compareObjects(a, b, aTableComponentModifiers.Constraints, bTableComponentModifiers.Constraints),
		compareObjects(a, b, aTableComponentModifiers.Indexes, bTableComponentModifiers.Indexes),
		compareObjects(a, b, wrapWithContextValue(a, a.Views, NewViewModifier), wrapWithContextValue(b, b.Views, NewViewModifier)),
		compareObjects(a, b, wrapWithContextValue(a, a.Triggers, NewTriggerModifier), wrapWithContextValue(b, b.Triggers, NewTriggerModifier)),
	} {
		for _, statement := range statements {
			key := strings.Join([]string{
				statement.statementType,
				statement.objectType,
				statement.key,
			}, "::")

			uniqueStatements[key] = statement
		}
	}

	var unorderedStatements []ddlStatement
	for _, statement := range uniqueStatements {
		unorderedStatements = append(unorderedStatements, statement)
	}

	filter := func(statementType, objectType string) []ddlStatement {
		var filtered []ddlStatement
		for _, statements := range unorderedStatements {
			if statements.statementType == statementType && statements.objectType == objectType {
				filtered = append(filtered, statements)
			}
		}

		return filtered
	}

	// Dependency mapping:
	//
	// extensions  : no dependencies
	// enums       : no dependencies
	// functions   : no dependencies
	// tables      : no dependencies
	// sequences   : no dependencies
	// columns     : depends on tables, enums, sequences
	// constraints : depends on tables, columns; fk constraints depend on unique constraints
	// indexes     : depends on tables, columns
	// views       : depends on tables, columns, views
	// triggers    : depends on tables, columns, functions

	sortByKey := func(statements []ddlStatement) {
		slices.SortFunc(statements, func(a, b ddlStatement) int {
			return cmp.Compare(a.key, b.key)
		})
	}

	sortByClosure := func(cls closure) func([]ddlStatement) {
		return func(statements []ddlStatement) {
			statementsByKey := map[string]ddlStatement{}
			for _, stmt := range statements {
				statementsByKey[stmt.key] = stmt
			}

			// Build a graph where nodes are statement keys and edges are
			// transitive references between them. Edges are directed from
			// the reference to the referencee.
			graph := map[string]map[string]struct{}{}
			for _, stmt := range statements {
				// Ensure the graph contains all keys.
				graph[stmt.key] = map[string]struct{}{}
			}
			for _, stmt := range statements {
				for reference := range cls[stmt.key] {
					if _, ok := graph[reference]; ok {
						graph[reference][stmt.key] = struct{}{}
					}
				}
			}

			// Build a topological ordering of the statements where ties
			// are broken by keys in lexicographic order.
			topologicalOrder := make([]ddlStatement, 0, len(statements))
			for len(graph) > 0 {
				// Gather all keys with no remaining dependencies.
				//
				// The textbook implementation would use a min queue to quickly select the
				// key with no adjacent edges, but the size of the data here should be small
				// enough that that scanning the (shrinking) graph on each iteration is fine.
				var candidates []string
				for key, edges := range graph {
					if len(edges) == 0 {
						candidates = append(candidates, key)
					}
				}
				if len(candidates) == 0 {
					panic("cycle detected in closure, cannot perform topological sort")
				}

				// Select the next key and add it to the topological order.
				sort.Strings(candidates)
				top := candidates[0]
				topologicalOrder = append(topologicalOrder, statementsByKey[top])

				// Remove the key from the node and edge sets.
				delete(graph, top)
				for _, edges := range graph {
					delete(edges, top)
				}
			}

			// Update the statements in-place to reflect the new order.
			for i := range statements {
				statements[i] = topologicalOrder[i]
			}
		}
	}

	createDependencyClosure, dropDependencyClosure := viewDependencyClosures(a, b)
	sortCreateViews := sortByClosure(createDependencyClosure)
	sortDropViews := sortByClosure(dropDependencyClosure)

	order := []struct {
		statementType string
		objectType    string
		order         func(statements []ddlStatement)
	}{
		{"drop", "trigger", sortByKey},
		{"drop", "view", sortDropViews},
		{"drop", "constraint", sortDropViews},
		{"drop", "index", sortByKey},
		{"drop", "column", sortByKey},
		{"drop", "sequence", sortByKey},
		{"drop", "table", sortByKey},
		{"drop", "function", sortByKey},
		{"drop", "enum", sortByKey},
		{"drop", "extension", sortByKey},
		{"create", "extension", sortByKey},
		{"create", "enum", sortByKey},
		{"replace", "enum", sortByKey},
		{"create", "function", sortByKey},
		{"replace", "function", sortByKey},
		{"create", "table", sortByKey},
		{"create", "sequence", sortByKey},
		{"replace", "sequence", sortByKey},
		{"create", "column", sortByKey},
		{"replace", "column", sortByKey},
		{"create", "index", sortByKey},
		{"create", "constraint", sortByKey},
		{"create", "view", sortCreateViews},
		{"create", "trigger", sortByKey},
	}

	for _, o := range order {
		filtered := filter(o.statementType, o.objectType)
		o.order(filtered)

		for _, statement := range filtered {
			statements = append(statements, statement.statements...)
		}
	}

	return statements
}

//
//
//

func viewDependencyClosures(a, b SchemaDescription) (createDependencyClosure, dropDependencyClosure closure) {
	createDependencyClosure = closure{}
	for _, dependency := range a.ColumnDependencies {
		sourceKey := fmt.Sprintf("%q.%q", dependency.SourceNamespace, dependency.SourceTableOrViewName)
		dependencyKey := fmt.Sprintf("%q.%q", dependency.UsedNamespace, dependency.UsedTableOrView)

		if _, ok := createDependencyClosure[sourceKey]; !ok {
			createDependencyClosure[sourceKey] = map[string]struct{}{}
		}

		createDependencyClosure[sourceKey][dependencyKey] = struct{}{}
	}

	dropDependencyClosure = closure{}
	for _, dependency := range b.ColumnDependencies {
		sourceKey := fmt.Sprintf("%q.%q", dependency.SourceNamespace, dependency.SourceTableOrViewName)
		dependencyKey := fmt.Sprintf("%q.%q", dependency.UsedNamespace, dependency.UsedTableOrView)

		if _, ok := dropDependencyClosure[dependencyKey]; !ok {
			dropDependencyClosure[dependencyKey] = map[string]struct{}{}
		}

		dropDependencyClosure[dependencyKey][sourceKey] = struct{}{}
	}

	transitiveClosure(createDependencyClosure)
	transitiveClosure(dropDependencyClosure)

	return createDependencyClosure, dropDependencyClosure
}

//
//

// closure is a reference relationship mapping a key to a set of references.
type closure map[string]map[string]struct{}

// transitiveClosure expands the given closure in-place to directly encode
// all transitive references.
func transitiveClosure(cls closure) {
	changed := true
	for changed {
		changed = false

		for _, references := range cls {
			for oldReference := range references {
				for newReference := range cls[oldReference] {
					if _, ok := references[newReference]; !ok {
						references[newReference] = struct{}{}
						changed = true
					}
				}
			}
		}
	}
}

//
//
//

type keyer interface {
	Key() string
}

type equaler[T any] interface {
	Equals(T) bool
}

type modifier[T equaler[T]] interface {
	keyer
	ObjectType() string
	Description() T
	Create() string
	Drop() string
}

type alterer[T any] interface {
	AlterExisting(existingSchema SchemaDescription, existingObject T) ([]ddlStatement, bool)
}

type ddlStatement struct {
	key           string
	statementType string
	objectType    string
	statements    []string
}

func newStatement(key string, statementType, objectType string, statements ...string) ddlStatement {
	return ddlStatement{
		key:           key,
		statementType: statementType,
		objectType:    objectType,
		statements:    statements,
	}
}

func compareObjects[T equaler[T], M modifier[T]](a, b SchemaDescription, as, bs []M) (statements []ddlStatement) {
	missing, additional, common := partition(as, bs)

	for _, modifier := range missing {
		statements = append(statements, newStatement(
			modifier.Key(),
			"create",
			modifier.ObjectType(),
			modifier.Create(),
		))
	}

	for _, modifier := range additional {
		statements = append(statements, newStatement(
			modifier.Key(),
			"drop",
			modifier.ObjectType(),
			modifier.Drop(),
		))
	}

	for _, pair := range common {
		var (
			aModifier    = pair.a
			bModifier    = pair.b
			aDescription = aModifier.Description()
			bDescription = bModifier.Description()
		)

		if aDescription.Equals(bDescription) {
			continue
		}

		if alterer, ok := any(aModifier).(alterer[T]); ok {
			if alterStatements, ok := alterer.AlterExisting(b, bDescription); ok {
				statements = append(statements, alterStatements...)
				continue
			}
		}

		statements = append(statements, newStatement(bModifier.Key(), "drop", bModifier.ObjectType(), bModifier.Drop()))
		statements = append(statements, newStatement(aModifier.Key(), "create", aModifier.ObjectType(), aModifier.Create()))
	}

	return statements
}

//
//
//

type pair[T any] struct {
	a, b T
}

// missing = present in a but not b
// additional = present in b but not a
func partition[T keyer](a, b []T) (missing, additional []T, common []pair[T]) {
	aMap := map[string]T{}
	for _, value := range a {
		aMap[value.Key()] = value
	}

	bMap := map[string]T{}
	for _, value := range b {
		bMap[value.Key()] = value
	}

	for key, aValue := range aMap {
		if bValue, ok := bMap[key]; ok {
			common = append(common, pair[T]{aValue, bValue})
		} else {
			missing = append(missing, aValue)
		}
	}

	for key, bValue := range bMap {
		if _, ok := aMap[key]; !ok {
			additional = append(additional, bValue)
		}
	}

	return missing, additional, common
}

//
//
//

func wrap[T, R any](s []T, f func(T) R) (wrapped []R) {
	for _, value := range s {
		wrapped = append(wrapped, f(value))
	}

	return wrapped
}

func wrapWithContextValue[C, T, R any](c C, s []T, f func(C, T) R) []R {
	return wrap(s, func(v T) R { return f(c, v) })
}

func wrapWithContextValues[C1, C2, T, R any](c1 C1, c2 C2, s []T, f func(C1, C2, T) R) []R {
	return wrap(s, func(v T) R { return f(c1, c2, v) })
}
