package pgutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollector(t *testing.T) {
	db := NewTestDB(t)
	collector := NewCollector[int](NewAnyValueScanner[int]())
	scanner := NewRowScanner(collector.Scanner())

	require.NoError(t, scanner(db.Query(context.Background(), RawQuery(`SELECT * FROM (VALUES (1), (2), (3)) AS t(number)`))))
	require.NoError(t, scanner(db.Query(context.Background(), RawQuery(`SELECT * FROM (VALUES (4), (5), (6)) AS t(number)`))))
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, collector.Slice())
}
