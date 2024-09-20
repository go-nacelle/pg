package pgutil

import "errors"

type ScanFunc func(Scanner) error
type MaybeScanFunc func(Scanner) (bool, error)

func newMaybeScanFunc(f ScanFunc) MaybeScanFunc {
	return func(s Scanner) (bool, error) {
		return true, f(s)
	}
}

type RowScannerFunc func(rows Rows, queryErr error) error

func NewRowScanner(f ScanFunc) RowScannerFunc {
	return NewMaybeRowScanner(newMaybeScanFunc(f))
}

func NewMaybeRowScanner(f MaybeScanFunc) RowScannerFunc {
	return func(rows Rows, queryErr error) (err error) {
		if queryErr != nil {
			return queryErr
		}
		defer func() { err = errors.Join(err, rows.Close(), rows.Err()) }()

		for rows.Next() {
			if ok, err := f(rows); err != nil {
				return err
			} else if !ok {
				break
			}
		}

		return nil
	}
}
