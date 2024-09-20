package pgutil

import (
	"context"
	"slices"

	"github.com/lib/pq"
)

type FunctionDescription struct {
	Namespace  string
	Name       string
	Definition string
	ArgTypes   []string
}

func (d FunctionDescription) Equals(other FunctionDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name &&
		slices.Equal(d.ArgTypes, other.ArgTypes) &&
		d.Definition == other.Definition
}

var scanFunctions = NewSliceScanner(func(s Scanner) (f FunctionDescription, _ error) {
	err := s.Scan(&f.Namespace, &f.Name, &f.Definition, pq.Array(&f.ArgTypes))
	return f, err
})

func DescribeFunctions(ctx context.Context, db DB) ([]FunctionDescription, error) {
	return scanFunctions(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			p.proname AS name,
			pg_get_functiondef(p.oid) AS definition,
			COALESCE(
				ARRAY(
					SELECT typ.typname
					FROM unnest(p.proargtypes) AS t(type_oid)
					JOIN pg_type typ ON typ.oid = t.type_oid
				),
			'{}'::text[]) AS argtypes
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_language l ON l.oid = p.prolang AND l.lanname IN ('sql', 'plpgsql')
		WHERE
			n.nspname NOT LIKE 'pg_%' AND
			n.nspname != 'information_schema' AND
			-- function is defined outside of any active extension
			NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')
		ORDER BY
			n.nspname,
			p.proname
	`)))
}
