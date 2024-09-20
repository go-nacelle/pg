package pgutil

import (
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	testQuery := func(t *testing.T, q Q, expectedQuery string, expectedArgs ...any) {
		t.Helper()

		query, args := q.Format()
		assert.Equal(t, expectedQuery, query)
		assert.Equal(t, expectedArgs, args)
	}

	t.Run("literal", func(t *testing.T) {
		q := Query("SELECT random()", Args{
			// empty
		})

		testQuery(t, q, "SELECT random()")
	})

	t.Run("simple", func(t *testing.T) {
		q := Query("SELECT * FROM users WHERE id = {:id}", Args{
			"id": 42,
		})

		testQuery(t, q, "SELECT * FROM users WHERE id = $1", 42)
	})

	t.Run("quoted", func(t *testing.T) {
		q := Query("SELECT {:col} FROM users", Args{
			"col": Quote("username"),
		})

		testQuery(t, q, "SELECT username FROM users")
	})

	t.Run("variable reuse", func(t *testing.T) {
		q := Query("SELECT * FROM users WHERE (id = {:id} AND NOT blocked) OR (id != {:id} AND admin)", Args{
			"id": 42,
		})

		testQuery(t, q, "SELECT * FROM users WHERE (id = $1 AND NOT blocked) OR (id != $1 AND admin)", 42)
	})

	t.Run("fragments", func(t *testing.T) {
		cond := Query("WHERE name = {:name} AND age = {:age}", Args{
			"name": "efritz",
			"age":  34,
		})

		limit := Query("LIMIT {:limit} OFFSET {:offset}", Args{
			"limit":  10,
			"offset": 20,
		})

		q := Query("SELECT name FROM users {:cond} {:limit}", Args{
			"cond":  cond,
			"limit": limit,
		})

		testQuery(t, q,
			"SELECT name FROM users WHERE name = $1 AND age = $2 LIMIT $3 OFFSET $4",
			"efritz", 34, 10, 20,
		)
	})

	t.Run("nested subqueries", func(t *testing.T) {
		preferredKeys := pq.Array([]string{"foo", "bar", "baz"})
		selectSubquery := Query("SELECT * FROM pairs WHERE s.key IN {:prefer}", Args{
			"prefer": preferredKeys,
		})

		avoidedKeys := pq.Array([]string{"bonk", "quux", "honk"})
		condSubquery := Query("SELECT s.value FROM pairs WHERE s.key IN {:avoid}", Args{
			"avoid": avoidedKeys,
		})

		q := Query("SELECT {:lit}, s.key, s.value FROM ({:selectSubquery}) s WHERE s.key != {:avoid} s.value NOT IN ({:condSubquery})", Args{
			"lit":            "test",
			"selectSubquery": selectSubquery,
			"avoid":          "__invalid",
			"condSubquery":   condSubquery,
		})

		testQuery(t, q,
			"SELECT $1, s.key, s.value FROM (SELECT * FROM pairs WHERE s.key IN $2) s WHERE s.key != $3 s.value NOT IN (SELECT s.value FROM pairs WHERE s.key IN $4)",
			"test", preferredKeys, "__invalid", avoidedKeys,
		)
	})

	t.Run("nested nested subqueries", func(t *testing.T) {
		q1 := Query("SELECT {:value}", Args{"value": "foo"})
		q2 := Query("SELECT z FROM inner WHERE x = {:value} AND y = ({:q})", Args{"value": "bar", "q": q1})
		q3 := Query("SELECT w FROM outer WHERE x = {:value} AND y = ({:q})", Args{"value": "baz", "q": q2})

		testQuery(t, q3,
			"SELECT w FROM outer WHERE x = $1 AND y = (SELECT z FROM inner WHERE x = $2 AND y = (SELECT $3))",
			"baz", "bar", "foo",
		)
	})

	t.Run("literal percent operator", func(t *testing.T) {
		q := Query("SELECT * FROM index WHERE a <<% {:term} AND document_id = {:documentID}", Args{
			"term":       "how to delete someone else's tweet",
			"documentID": 42,
		})

		testQuery(t, q, "SELECT * FROM index WHERE a <<% $1 AND document_id = $2", "how to delete someone else's tweet", 42)
	})

	t.Run("literal arrays", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			q := Query("SELECT * FROM products WHERE tag IN '{}'", Args{
				// empty
			})

			testQuery(t, q, "SELECT * FROM products WHERE tag IN '{}'")
		})

		t.Run("singleton", func(t *testing.T) {
			q := Query("SELECT * FROM products WHERE tag NOT IN '{uselessjunk}'", Args{
				// empty
			})

			testQuery(t, q, "SELECT * FROM products WHERE tag NOT IN '{uselessjunk}'")
		})

		t.Run("multi value", func(t *testing.T) {
			q := Query("SELECT * FROM products WHERE tag IN '{sale,electronics}'", Args{
				// empty
			})

			testQuery(t, q, "SELECT * FROM products WHERE tag IN '{sale,electronics}'")
		})
	})
}
