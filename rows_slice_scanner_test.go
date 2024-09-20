package pgutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSliceScanner(t *testing.T) {
	t.Run("scalar values", func(t *testing.T) {
		values, err := ScanInts(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1), (2), (3)) AS t(number)`),
		))
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, values)
	})

	t.Run("custom struct values", func(t *testing.T) {
		values, err := scanTestPairs(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1,2), (2,3), (3,4)) AS t(x,y)`),
		))
		require.NoError(t, err)
		assert.Equal(t, []testPair{{1, 2}, {2, 3}, {3, 4}}, values)
	})

	t.Run("no values", func(t *testing.T) {
		values, err := ScanInts(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1), (2), (3)) AS t(number) LIMIT 0`),
		))
		require.NoError(t, err)
		assert.Empty(t, values)
	})
}

func TestMaybeSliceScanner(t *testing.T) {
	values, err := scanMaybeTestPairs(NewTestDB(t).Query(context.Background(),
		RawQuery(`SELECT * FROM (VALUES (1,2), (2,3), (0,0), (3,4)) AS t(x,y)`),
	))
	require.NoError(t, err)
	assert.Equal(t, []testPair{{1, 2}, {2, 3}}, values)
}

func TestFirstScanner(t *testing.T) {
	t.Run("scalar value", func(t *testing.T) {
		value, ok, err := ScanInt(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1)) AS t(number)`),
		))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 1, value)
	})

	t.Run("scalar value (ignores non-first values)", func(t *testing.T) {
		value, ok, err := ScanInt(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1), (2), (3)) AS t(number)`),
		))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 1, value)
	})

	t.Run("custom struct value", func(t *testing.T) {
		value, ok, err := scanFirstTestPair(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1,2)) AS t(x,y)`),
		))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, testPair{1, 2}, value)
	})

	t.Run("no value", func(t *testing.T) {
		_, ok, err := ScanInt(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1), (2), (3)) AS t(number) LIMIT 0`),
		))
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestMaybeFirstScanner(t *testing.T) {
	t.Run("custom struct value", func(t *testing.T) {
		value, ok, err := scanMaybeFirstTestPair(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (1,2)) AS t(x,y)`),
		))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, testPair{1, 2}, value)
	})

	t.Run("rejected value", func(t *testing.T) {
		type testPair struct {
			x int
			y int
		}
		scanner := NewMaybeFirstScanner(func(s Scanner) (p testPair, _ bool, _ error) {
			err := s.Scan(&p.x, &p.y)
			return p, p.x != 0 && p.y != 0, err
		})

		_, ok, err := scanner(NewTestDB(t).Query(context.Background(),
			RawQuery(`SELECT * FROM (VALUES (0,0), (1,2)) AS t(x,y)`),
		))
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

//
//
//

type testPair struct {
	x int
	y int
}

var scanTestPairs = NewSliceScanner(func(s Scanner) (p testPair, _ error) {
	err := s.Scan(&p.x, &p.y)
	return p, err
})

var scanMaybeTestPairs = NewMaybeSliceScanner(func(s Scanner) (p testPair, _ bool, _ error) {
	err := s.Scan(&p.x, &p.y)
	return p, p.x != 0 && p.y != 0, err
})

var scanFirstTestPair = NewFirstScanner(func(s Scanner) (p testPair, _ error) {
	err := s.Scan(&p.x, &p.y)
	return p, err
})

var scanMaybeFirstTestPair = NewMaybeFirstScanner(func(s Scanner) (p testPair, _ bool, _ error) {
	err := s.Scan(&p.x, &p.y)
	return p, p.x != 0 && p.y != 0, err
})
