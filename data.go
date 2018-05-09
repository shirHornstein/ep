package ep

import (
	"sort"
)

// Data is an abstract interface representing a set of typed values. Implement
// it for each type of data that you need to support.
// NOTE: all indices received as arguments should be in range [0, Len())
type Data interface {
	// Type returns the data type of the contained values
	Type() Type

	sort.Interface // data is sortable

	// Slice returns a new data object containing only the values from the start
	// to end indices
	Slice(start, end int) Data

	// Append another data object to this one. It can be assumed that the type of
	// the input data is similar to the current one, otherwise it's safe to panic
	Append(Data) Data

	// Duplicate this object t times and returns new data with Len() * t rows
	Duplicate(t int) Data

	// IsNull checks if the given index contains null
	IsNull(i int) bool

	// MarkNull sets the value in the given index to null
	MarkNull(i int)

	// Nulls returns a booleans array indicates whether the i-th value is null
	Nulls() []bool

	// Equal checks if another data object refer to same underlying data
	// as this one (shallow comparison)
	Equal(Data) bool

	// Copy single row from given data at fromRow position to this data, located
	// at toRow position.
	// Equivalent to this[toRow] = from.Slice(fromRow, fromRow+1)
	Copy(from Data, fromRow, toRow int)

	// Strings returns the string representation of all of the Data values
	Strings() []string
}

// Clone the contents of the provided Data. Dataset also implements the Data
// interface is a valid input to this function.
func Clone(data Data) Data {
	return data.Type().Data(0).Append(data)
}

// Cut the Data into several sub-segments at the provided cut-point indices. It's
// effectively the same as calling Data.Slice() multiple times. Dataset also
// implements the Data interface is a valid input to this function.
func Cut(data Data, cutpoints ...int) Data {
	res := []Data{}
	var last int
	for _, i := range cutpoints {
		res = append(res, data.Slice(last, i))
		last = i
	}

	if last < data.Len() {
		res = append(res, data.Slice(last, data.Len()))
	}

	return NewDataset(res...)
}
