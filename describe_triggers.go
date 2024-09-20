package pgutil

import "context"

type TriggerDescription struct {
	Namespace         string
	Name              string
	TableName         string
	FunctionNamespace string
	Definition        string
}

func (d TriggerDescription) Equals(other TriggerDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name &&
		d.TableName == other.TableName &&
		d.FunctionNamespace == other.FunctionNamespace &&
		d.Definition == other.Definition
}

var scanTriggers = NewSliceScanner(func(s Scanner) (t TriggerDescription, _ error) {
	err := s.Scan(&t.Namespace, &t.Name, &t.TableName, &t.FunctionNamespace, &t.Definition)
	return t, err
})

func DescribeTriggers(ctx context.Context, db DB) ([]TriggerDescription, error) {
	return scanTriggers(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			t.tgname AS name,
			c.relname AS table_name,
			tn.nspname AS function_namespace,
			pg_catalog.pg_get_triggerdef(t.oid, true) AS definition
		FROM pg_catalog.pg_trigger t
		JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
		JOIN pg_catalog.pg_namespace tn ON tn.oid = p.pronamespace
		WHERE NOT t.tgisinternal
		ORDER BY n.nspname, t.tgname, c.relname, tn.nspname
	`)))
}
