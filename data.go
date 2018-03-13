package ep

import (
	"sort"
)

// Data is an abstract interface representing a set of typed values. Implement
// it for each type of data that you need to support
type Data interface {
	// Type returns the data type of the contained values
	Type() Type

	sort.Interface // data is sortable

	// Slice returns a new data object containing only the values from the start
	// to end indices
	Slice(start, end int) Data

	// Append another data object to this one. It can be assumed that the type
	// of the input data is similar to the current one, otherwise it's safe to
	// panic
	Append(Data) Data

	// Duplicate duplicates this object t times and returns new data with
	// Len() * t rows
	Duplicate(t int) Data

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

// Partition the Data into several sub-segments such that each segment contains
// the same value, repeated multiple times. Dataset also implements the Data
// interface is a valid input to this function.
//
// NOTE that partitioning will re-order the data to create the minimum number of
// batches. Thus is does not maintain order
func Partition(data Data) Data {
	sort.Sort(data)
	var last string
	var cutpoints []int
	for i, s := range data.Strings() {
		if i == 0 {
			last = s
		}

		if last != s {
			cutpoints = append(cutpoints, i)
			last = s
		}
	}

	return Cut(data, cutpoints...)
}
