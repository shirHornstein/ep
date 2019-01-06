package ep

import (
	"github.com/panoplyio/ep/compare"
	"sort"
)

// Data is an abstract interface representing a set of typed values. Implement
// it for each type of data that you need to support.
// NOTES:
// 1. all indices received as arguments should be in range [0, Len())
// 2. besides Equal, all Data received as arguments should have same type as this
// 3. mutable functions (MarkNull, Swap, Copy, Append) should be called only within
//    creation scope or at gathering point. i.e. when no other runner have access
//    to this object. not following this rule will end up with data race
type Data interface {
	// Type returns the data type of the contained values
	Type() Type

	sort.Interface // data is sortable

	// LessOther reports whether thisRow-th element should sort before the
	// otherRow-th element in other data object
	LessOther(thisRow int, other Data, otherRow int) bool

	// Slice returns a new data object containing only the values from the start
	// to end indices
	Slice(start, end int) Data

	// Append takes another data object and appends it to this one.
	// It can be assumed that the type of the input data is similar
	// to the current one, otherwise it's safe to panic
	Append(other Data) Data

	// Duplicate returns new data object containing this object t
	// times. returned value has Len() * t rows
	Duplicate(t int) Data

	// IsNull checks if the given index contains null
	IsNull(i int) bool

	// MarkNull sets the value in the given index to null
	MarkNull(i int)

	// Nulls returns a booleans array indicates whether the i-th value is null
	Nulls() []bool

	// Equal checks if another data object refer to same underlying data
	// as this one (shallow comparison)
	Equal(other Data) bool

	// Compare checks current object data to other data object
	Compare(other Data) ([]compare.Comparison, error)

	// Copy copies single row from given data at fromRow position to this data,
	// located at toRow position.
	// Equivalent to this[toRow] = from.Slice(fromRow, fromRow+1)
	Copy(from Data, fromRow, toRow int)

	// Strings returns the string representation of all of the Data values
	Strings() []string
}

// Cut the Data into several sub-segments at the provided cut-point indices. It's
// effectively the same as calling Data.Slice() multiple times
func Cut(data Data, cutPoints ...int) []Data {
	var res []Data
	var last int
	for _, i := range cutPoints {
		res = append(res, data.Slice(last, i))
		last = i
	}

	if last < data.Len() {
		res = append(res, data.Slice(last, data.Len()))
	}

	return res
}
