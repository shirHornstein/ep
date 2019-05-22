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
// 3. mutable functions (MarkNull, Swap, Copy) should be called only within
//    creation scope or at gathering point, i.e when no other runners have access
//    to this object. Not following this rule will end up with data race
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

	// Compare compares given data to this data, row by row
	Compare(other Data) ([]compare.Result, error)

	// Copy copies single row from given data at fromRow position to this data,
	// located at toRow position.
	// Equivalent to this[toRow] = from.Slice(fromRow, fromRow+1)
	Copy(from Data, fromRow, toRow int)

	// CopyNTimes copies len(dup) rows from given data from fromRow position to this data,
	// located at toRow position.
	// i-th line will be copied dup[i] times.
	// this[toRow:] is expected to have capacity larger than sum of all duplications
	CopyNTimes(from Data, fromRow, toRow int, duplications []int)

	// CopyByIndexes copies len(fromRows) rows from given data for all fromRow[i] position to this data,
	// located at toRow position.
	// this[toRow:] is expected to have capacity larger than len(fromRows)
	CopyByIndexes(from Data, fromRows []int, toRow int)

	// Strings returns the string representation of all of the Data values
	Strings() []string
}

// DataBuilder exposes an efficient way to build Data objects by appending
// smaller Data objects. DataBuilders should be used every time there is a need
// to Append multiple Data objects.
//
// Append() method can be called multiple times without significant
// allocations. Calling Data() will return a new Data object containing all
// appended Data objects.
type DataBuilder interface {
	// Append appends data to the data that this DataBuilder has
	Append(data Data)

	// Data returns a new Data object containing all appended Data objects in
	// the same order. Subsequent Append() calls on this builder do not affect
	// the returned Data object. Calling Data() on DataBuilder objects without
	// preceding Append() calls is not allowed, and the behavior in such cases
	// is undefined
	Data() Data
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
