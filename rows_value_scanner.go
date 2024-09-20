package pgutil

type ScanValueFunc[T any] func(Scanner) (T, error)
type MaybeScanValueFunc[T any] func(Scanner) (T, bool, error)

func newMaybeScanValueFunc[T any](f ScanValueFunc[T]) MaybeScanValueFunc[T] {
	return func(s Scanner) (T, bool, error) {
		value, err := f(s)
		return value, true, err
	}
}

func NewAnyValueScanner[T any]() ScanValueFunc[T] {
	return func(s Scanner) (value T, err error) {
		err = s.Scan(&value)
		return
	}
}

type Collector[T any] struct {
	scanner ScanValueFunc[T]
	values  []T
}

func NewCollector[T any](scanner ScanValueFunc[T]) *Collector[T] {
	return &Collector[T]{
		scanner: NewAnyValueScanner[T](),
	}
}

func (c *Collector[T]) Scanner() ScanFunc {
	return func(s Scanner) error {
		value, err := c.scanner(s)
		c.values = append(c.values, value)
		return err
	}
}

func (c *Collector[T]) Slice() []T {
	return c.values
}
