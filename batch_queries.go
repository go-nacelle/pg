package pgutil

import (
	"fmt"
	"strings"
	"sync"
)

var (
	placeholders           []string
	placeholdersCache      = map[int]string{}
	placeholdersCacheMutex sync.Mutex
)

func init() {
	placeholders = make([]string, 0, maxNumPostgresParameters)

	for i := 0; i < maxNumPostgresParameters; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%05d", i+1))
	}
}

type batchQueryBuilder struct {
	numColumns   int
	queryPrefix  string
	querySuffix  string
	placeholders string
}

func newBatchQueryBuilder(tableName string, columnNames []string, onConflictClause, returningClause string) *batchQueryBuilder {
	var (
		numColumns  = len(columnNames)
		queryPrefix = fmt.Sprintf("INSERT INTO %q (%s) VALUES", tableName, strings.Join(quoteColumnNames(columnNames), ", "))
		querySuffix = fmt.Sprintf("%s %s", onConflictClause, returningClause)
		all         = makeBatchPlaceholdersString(numColumns)
	)

	return &batchQueryBuilder{
		numColumns:   numColumns,
		queryPrefix:  queryPrefix,
		querySuffix:  querySuffix,
		placeholders: all,
	}
}

func (b *batchQueryBuilder) build(batchSize int) string {
	return fmt.Sprintf("%s %s %s", b.queryPrefix, b.placeholders[:placeholdersLen(b.numColumns, batchSize)], b.querySuffix)
}

func makeBatchPlaceholdersString(numColumns int) string {
	placeholdersCacheMutex.Lock()
	defer placeholdersCacheMutex.Unlock()
	if placeholders, ok := placeholdersCache[numColumns]; ok {
		return placeholders
	}

	var sb strings.Builder
	sb.WriteString("(")
	sb.WriteString(placeholders[0])
	for i := 1; i < maxNumPostgresParameters; i++ {
		if i%numColumns == 0 {
			sb.WriteString("),(")
		} else {
			sb.WriteString(",")
		}

		sb.WriteString(placeholders[i])
	}
	sb.WriteString(")")

	placeholders := sb.String()
	placeholdersCache[numColumns] = placeholders
	return placeholders
}

func placeholdersLen(numColumns, batchSize int) int {
	var (
		numRows        = batchSize / numColumns
		placeholderLen = 6                                           // e.g., `$00123`
		rowLen         = sequenceLen(numColumns, placeholderLen) + 2 // e.g., `($00123,$001234,...)`
		totalLen       = sequenceLen(numRows, rowLen)
	)

	return totalLen
}

func sequenceLen(num, len int) int {
	return num*(len+1) - 1
}

func quoteColumnNames(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteColumnName(name)
	}

	return quoted
}

func quoteColumnName(name string) string {
	return fmt.Sprintf("%q", name)
}
