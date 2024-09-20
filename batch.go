package pgutil

import (
	"context"
	"fmt"
)

type BatchInserter struct {
	db               DB
	numColumns       int
	maxBatchSize     int
	maxCapacity      int
	queryBuilder     *batchQueryBuilder
	returningScanner ScanFunc
	values           []any
}

const maxNumPostgresParameters = 65535

func NewBatchInserter(db DB, tableName string, columnNames []string, configs ...BatchInserterConfigFunc) *BatchInserter {
	var (
		options          = getBatchInserterOptions(configs)
		numColumns       = len(columnNames)
		maxBatchSize     = int(maxNumPostgresParameters/numColumns) * numColumns
		maxCapacity      = maxBatchSize + numColumns
		queryBuilder     = newBatchQueryBuilder(tableName, columnNames, options.onConflictClause, options.returningClause)
		returningScanner = options.returningScanner
	)

	return &BatchInserter{
		db:               db,
		numColumns:       numColumns,
		maxBatchSize:     maxBatchSize,
		maxCapacity:      maxCapacity,
		queryBuilder:     queryBuilder,
		returningScanner: returningScanner,
		values:           make([]any, 0, maxCapacity),
	}
}

func (i *BatchInserter) Insert(ctx context.Context, values ...any) error {
	if len(values) != i.numColumns {
		return fmt.Errorf("received %d values for %d columns", len(values), i.numColumns)
	}

	i.values = append(i.values, values...)

	if len(i.values) >= i.maxBatchSize {
		return i.Flush(ctx)
	}

	return nil
}

func (i *BatchInserter) Flush(ctx context.Context) error {
	if len(i.values) == 0 {
		return nil
	}

	n := i.maxBatchSize
	if len(i.values) < i.maxBatchSize {
		n = len(i.values)
	}

	batch := i.values[:n]
	i.values = append(make([]any, 0, i.maxCapacity), i.values[n:]...)

	batchSize := len(batch)
	query := i.queryBuilder.build(batchSize)
	return NewRowScanner(i.returningScanner)(i.db.Query(ctx, RawQuery(query, batch...)))
}
