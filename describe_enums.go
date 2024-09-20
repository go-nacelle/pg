package pgutil

import (
	"context"

	"github.com/lib/pq"
	"golang.org/x/exp/slices"
)

type EnumDescription struct {
	Namespace string
	Name      string
	Labels    []string
}

func (d EnumDescription) Equals(other EnumDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name &&
		slices.Equal(d.Labels, other.Labels)
}

var scanEnums = NewSliceScanner(func(s Scanner) (l EnumDescription, _ error) {
	err := s.Scan(&l.Namespace, &l.Name, pq.Array(&l.Labels))
	return l, err
})

func DescribeEnums(ctx context.Context, db DB) ([]EnumDescription, error) {
	return scanEnums(db.Query(ctx, RawQuery(`
		SELECT
			n.nspname AS namespace,
			t.typname AS name,
			array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
		FROM pg_catalog.pg_enum e
		JOIN pg_catalog.pg_type t ON t.oid = e.enumtypid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		GROUP BY n.nspname, t.typname
		ORDER BY n.nspname, t.typname
	`)))
}
