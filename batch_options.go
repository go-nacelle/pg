package pgutil

import (
	"fmt"
	"strings"
)

type (
	batchInserterOptions struct {
		onConflictClause string
		returningClause  string
		returningScanner ScanFunc
	}

	BatchInserterConfigFunc func(*batchInserterOptions)
)

func getBatchInserterOptions(configs []BatchInserterConfigFunc) *batchInserterOptions {
	options := &batchInserterOptions{}
	for _, f := range configs {
		f(options)
	}

	return options
}

func WithBatchInserterOnConflict(clause string) BatchInserterConfigFunc {
	return func(o *batchInserterOptions) {
		o.onConflictClause = fmt.Sprintf("ON CONFLICT %s", clause)
	}
}

func WithBatchInserterReturn(columns []string, scanner ScanFunc) BatchInserterConfigFunc {
	return func(o *batchInserterOptions) {
		o.returningClause = fmt.Sprintf("RETURNING %s", strings.Join(quoteColumnNames(columns), ", "))
		o.returningScanner = scanner
	}
}
