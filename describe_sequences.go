package pgutil

import (
	"context"
)

type SequenceDescription struct {
	Namespace    string
	Name         string
	Type         string
	StartValue   int
	MinimumValue int
	MaximumValue int
	Increment    int
	CycleOption  string
}

func (d SequenceDescription) Equals(other SequenceDescription) bool {
	return true &&
		d.Namespace == other.Namespace &&
		d.Name == other.Name &&
		d.Type == other.Type &&
		d.StartValue == other.StartValue &&
		d.MinimumValue == other.MinimumValue &&
		d.MaximumValue == other.MaximumValue &&
		d.Increment == other.Increment &&
		d.CycleOption == other.CycleOption
}

var scanSequences = NewSliceScanner(func(s Scanner) (l SequenceDescription, _ error) {
	err := s.Scan(
		&l.Namespace,
		&l.Name,
		&l.Type,
		&l.StartValue,
		&l.MinimumValue,
		&l.MaximumValue,
		&l.Increment,
		&l.CycleOption,
	)
	return l, err
})

func DescribeSequences(ctx context.Context, db DB) ([]SequenceDescription, error) {
	return scanSequences(db.Query(ctx, RawQuery(`
		SELECT
			s.sequence_schema AS namespace,
			s.sequence_name AS name,
			s.data_type AS type,
			s.start_value AS start_value,
			s.minimum_value AS minimum_value,
			s.maximum_value AS maximum_value,
			s.increment AS increment,
			s.cycle_option AS cycle_option
		FROM information_schema.sequences s
		WHERE
			s.sequence_schema NOT LIKE 'pg_%' AND
			s.sequence_schema != 'information_schema'
		ORDER BY s.sequence_schema, s.sequence_name
	`)))
}
