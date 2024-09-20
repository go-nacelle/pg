package pgutil

import (
	"context"
)

type SchemaDescription struct {
	Extensions         []ExtensionDescription
	Enums              []EnumDescription
	Functions          []FunctionDescription
	Tables             []TableDescription
	Sequences          []SequenceDescription
	Views              []ViewDescription
	Triggers           []TriggerDescription
	EnumDependencies   []EnumDependency
	ColumnDependencies []ColumnDependency
}

func DescribeSchema(ctx context.Context, db DB) (SchemaDescription, error) {
	extensions, err := DescribeExtensions(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	enums, err := DescribeEnums(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	functions, err := DescribeFunctions(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	tables, err := DescribeTables(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	sequences, err := DescribeSequences(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	views, err := DescribeViews(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	triggers, err := DescribeTriggers(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	enumDependencies, err := DescribeEnumDependencies(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	columnDependencies, err := DescribeColumnDependencies(ctx, db)
	if err != nil {
		return SchemaDescription{}, err
	}

	return SchemaDescription{
		Extensions:         extensions,
		Enums:              enums,
		Functions:          functions,
		Tables:             tables,
		Sequences:          sequences,
		Views:              views,
		Triggers:           triggers,
		EnumDependencies:   enumDependencies,
		ColumnDependencies: columnDependencies,
	}, nil
}
