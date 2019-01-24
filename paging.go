package nacellepg

import (
	"fmt"
	"math"

	"github.com/jmoiron/sqlx"
)

type (
	PageMeta struct {
		Page     int
		PageSize int
	}

	PagedResultMeta struct {
		NumPages   int `json:"num_pages"`
		NumResults int `json:"num_results"`
	}
)

func (m *PageMeta) Limit() int {
	return m.PageSize
}

func (m *PageMeta) Offset() int {
	return m.PageSize * (m.Page - 1)
}

func PagedSelect(
	db *LoggingDB,
	meta *PageMeta,
	baseQuery string,
	target interface{},
	args ...interface{},
) (*PagedResultMeta, error) {
	var (
		total      int
		countQuery = fmt.Sprintf("select count(*) from (%s) q", baseQuery)
	)

	if err := sqlx.Get(db, &total, countQuery, args...); err != nil {
		return nil, handlePostgresError(err, "select error")
	}

	var (
		limitQuery = fmt.Sprintf("%s limit $%d offset $%d", baseQuery, len(args)+1, len(args)+2)
		limitArgs  = append(args, meta.Limit(), meta.Offset())
	)

	if err := sqlx.Select(db, target, limitQuery, limitArgs...); err != nil {
		return nil, handlePostgresError(err, "select error")
	}

	return &PagedResultMeta{
		NumResults: total,
		NumPages:   int(math.Ceil(float64(total) / float64(meta.Limit()))),
	}, nil
}
