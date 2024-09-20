package pgutil

import "time"

type SliceScannerFunc[T any] func(rows Rows, queryErr error) ([]T, error)
type FirstScannerFunc[T any] func(rows Rows, queryErr error) (T, bool, error)

func NewSliceScanner[T any](f ScanValueFunc[T]) SliceScannerFunc[T] {
	return NewMaybeSliceScanner(newMaybeScanValueFunc(f))
}

func NewMaybeSliceScanner[T any](f MaybeScanValueFunc[T]) SliceScannerFunc[T] {
	return func(rows Rows, queryErr error) ([]T, error) {
		values := make([]T, 0)
		scan := func(s Scanner) (bool, error) {
			value, ok, err := f(s)
			if err != nil {
				return false, err
			}
			if ok {
				values = append(values, value)
			}

			return ok, nil
		}

		err := NewMaybeRowScanner(scan)(rows, queryErr)
		return values, err
	}
}

func NewFirstScanner[T any](f ScanValueFunc[T]) FirstScannerFunc[T] {
	return NewMaybeFirstScanner[T](newMaybeScanValueFunc(f))
}

func NewMaybeFirstScanner[T any](f MaybeScanValueFunc[T]) FirstScannerFunc[T] {
	return func(rows Rows, queryErr error) (value T, called bool, _ error) {
		scan := func(s Scanner) (_ bool, err error) {
			value, called, err = f(s)
			return false, err
		}

		err := NewMaybeRowScanner(scan)(rows, queryErr)
		return value, called, err
	}
}

var (
	ScanAny           = NewFirstScanner(NewAnyValueScanner[any]())
	ScanAnys          = NewSliceScanner(NewAnyValueScanner[any]())
	ScanBool          = NewFirstScanner(NewAnyValueScanner[bool]())
	ScanBools         = NewSliceScanner(NewAnyValueScanner[bool]())
	ScanFloat32       = NewFirstScanner(NewAnyValueScanner[float32]())
	ScanFloat32s      = NewSliceScanner(NewAnyValueScanner[float32]())
	ScanFloat64       = NewFirstScanner(NewAnyValueScanner[float64]())
	ScanFloat64s      = NewSliceScanner(NewAnyValueScanner[float64]())
	ScanInt           = NewFirstScanner(NewAnyValueScanner[int]())
	ScanInts          = NewSliceScanner(NewAnyValueScanner[int]())
	ScanInt16         = NewFirstScanner(NewAnyValueScanner[int16]())
	ScanInt16s        = NewSliceScanner(NewAnyValueScanner[int16]())
	ScanInt32         = NewFirstScanner(NewAnyValueScanner[int32]())
	ScanInt32s        = NewSliceScanner(NewAnyValueScanner[int32]())
	ScanInt64         = NewFirstScanner(NewAnyValueScanner[int64]())
	ScanInt64s        = NewSliceScanner(NewAnyValueScanner[int64]())
	ScanInt8          = NewFirstScanner(NewAnyValueScanner[int8]())
	ScanInt8s         = NewSliceScanner(NewAnyValueScanner[int8]())
	ScanString        = NewFirstScanner(NewAnyValueScanner[string]())
	ScanStrings       = NewSliceScanner(NewAnyValueScanner[string]())
	ScanUint          = NewFirstScanner(NewAnyValueScanner[uint]())
	ScanUints         = NewSliceScanner(NewAnyValueScanner[uint]())
	ScanUint16        = NewFirstScanner(NewAnyValueScanner[uint16]())
	ScanUint16s       = NewSliceScanner(NewAnyValueScanner[uint16]())
	ScanUint32        = NewFirstScanner(NewAnyValueScanner[uint32]())
	ScanUint32s       = NewSliceScanner(NewAnyValueScanner[uint32]())
	ScanUint64        = NewFirstScanner(NewAnyValueScanner[uint64]())
	ScanUint64s       = NewSliceScanner(NewAnyValueScanner[uint64]())
	ScanUint8         = NewFirstScanner(NewAnyValueScanner[uint8]())
	ScanUint8s        = NewSliceScanner(NewAnyValueScanner[uint8]())
	ScanTimestamp     = NewFirstScanner(NewAnyValueScanner[time.Time]())
	ScanTimestamps    = NewSliceScanner(NewAnyValueScanner[time.Time]())
	ScanNilBool       = NewFirstScanner(NewAnyValueScanner[*bool]())
	ScanNilBools      = NewSliceScanner(NewAnyValueScanner[*bool]())
	ScanNilFloat32    = NewFirstScanner(NewAnyValueScanner[*float32]())
	ScanNilFloat32s   = NewSliceScanner(NewAnyValueScanner[*float32]())
	ScanNilFloat64    = NewFirstScanner(NewAnyValueScanner[*float64]())
	ScanNilFloat64s   = NewSliceScanner(NewAnyValueScanner[*float64]())
	ScanNilInt        = NewFirstScanner(NewAnyValueScanner[*int]())
	ScanNilInts       = NewSliceScanner(NewAnyValueScanner[*int]())
	ScanNilInt16      = NewFirstScanner(NewAnyValueScanner[*int16]())
	ScanNilInt16s     = NewSliceScanner(NewAnyValueScanner[*int16]())
	ScanNilInt32      = NewFirstScanner(NewAnyValueScanner[*int32]())
	ScanNilInt32s     = NewSliceScanner(NewAnyValueScanner[*int32]())
	ScanNilInt64      = NewFirstScanner(NewAnyValueScanner[*int64]())
	ScanNilInt64s     = NewSliceScanner(NewAnyValueScanner[*int64]())
	ScanNilInt8       = NewFirstScanner(NewAnyValueScanner[*int8]())
	ScanNilInt8s      = NewSliceScanner(NewAnyValueScanner[*int8]())
	ScanNilString     = NewFirstScanner(NewAnyValueScanner[*string]())
	ScanNilStrings    = NewSliceScanner(NewAnyValueScanner[*string]())
	ScanNilUint       = NewFirstScanner(NewAnyValueScanner[*uint]())
	ScanNilUints      = NewSliceScanner(NewAnyValueScanner[*uint]())
	ScanNilUint16     = NewFirstScanner(NewAnyValueScanner[*uint16]())
	ScanNilUint16s    = NewSliceScanner(NewAnyValueScanner[*uint16]())
	ScanNilUint32     = NewFirstScanner(NewAnyValueScanner[*uint32]())
	ScanNilUint32s    = NewSliceScanner(NewAnyValueScanner[*uint32]())
	ScanNilUint64     = NewFirstScanner(NewAnyValueScanner[*uint64]())
	ScanNilUint64s    = NewSliceScanner(NewAnyValueScanner[*uint64]())
	ScanNilUint8      = NewFirstScanner(NewAnyValueScanner[*uint8]())
	ScanNilUint8s     = NewSliceScanner(NewAnyValueScanner[*uint8]())
	ScanNilTimestamp  = NewFirstScanner(NewAnyValueScanner[*time.Time]())
	ScanNilTimestamps = NewSliceScanner(NewAnyValueScanner[*time.Time]())
)
