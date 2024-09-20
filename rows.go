package pgutil

type Scanner interface {
	Scan(dst ...any) error
}

type Rows interface {
	Scanner

	Next() bool
	Close() error
	Err() error
}
