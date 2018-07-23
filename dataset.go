package ep

import (
	"fmt"
	"strings"
)

var _ = registerGob(NewDataset(), &datasetType{})
var errMismatch = fmt.Errorf("mismatched number of rows")

type datasetType struct{}

func (sett *datasetType) Name() string         { return "Dataset" }
func (sett *datasetType) String() string       { return sett.Name() }
func (sett *datasetType) Data(n int) Data      { return make(dataset, n) }
func (sett *datasetType) DataEmpty(n int) Data { return make(dataset, 0, n) }

// Dataset is a composite Data interface, containing several internal Data
// objects. It's a Data in itself, but allows traversing and manipulating the
// contained Data instances
type Dataset interface {
	Data // It's a Data - you can use it anywhere you'd use a Data object

	// Width returns the number of Data instances (columns) in the set
	Width() int

	// At returns the Data instance at index i
	At(i int) Data

	// Expand returns new dataset composed of this dataset's columns and other's
	// columns. Number of rows of both datasets should be equal
	Expand(other Dataset) (Dataset, error)

	// Split divides dataset to two parts, where the second part width determined by
	// the given secondWidth argument
	Split(secondWidth int) (Dataset, Dataset)
}

type dataset []Data

// NewDataset creates a new Data object that's a horizontal composition of the
// provided Data objects
func NewDataset(data ...Data) Dataset {
	return dataset(data)
}

// Width of the dataset (number of columns)
func (set dataset) Width() int {
	return len(set)
}

// At returns the Data at index i
func (set dataset) At(i int) Data {
	return set[i]
}

// Expand returns new dataset with set and other's columns
func (set dataset) Expand(other Dataset) (Dataset, error) {
	if set == nil {
		return other, nil
	} else if other == nil {
		return set, nil
	}
	// when expanding with variadicNulls - don't force same length
	isAnyVariadicNulls := set.Len() < 0 || other.Len() < 0
	if set.Len() != other.Len() && !isAnyVariadicNulls {
		return nil, errMismatch
	}
	otherCols := other.(dataset)
	return append(set, otherCols...), nil
}

// Split returns two datasets, with requested second width
func (set dataset) Split(secondWidth int) (Dataset, Dataset) {
	if set.Width() < secondWidth {
		panic("Unable to split dataset - not enough columns")
	}
	firstWidth := set.Width() - secondWidth
	return set[:firstWidth], set[firstWidth:]
}

// see Data.Type
func (set dataset) Type() Type {
	return &datasetType{}
}

// see sort.Interface.
// Len of the dataset (number of rows). Assumed that all columns are of equal
// length, and thus only checks the first
func (set dataset) Len() int {
	if set == nil || len(set) == 0 {
		return 0
	}
	// return length of first non variadicNulls column
	for _, col := range set {
		colLength := col.Len()
		if colLength >= 0 {
			return colLength
		}
	}
	return set[0].Len()
}

// see sort.Interface.
// By default sorts by columns appearance, ascending.
// NOTE: Unsafe for use with recurring columns. Consider use Sort(dataset,..) instead
func (set dataset) Less(i, j int) bool {
	var iLessThanJ bool
	for _, col := range set {
		iLessThanJ = col.Less(i, j)
		// iLessThanJ will be false also for equal values.
		// if Less(l, j) and Less(j, i) are both false, values are equal. Therefore
		// keep checking next sorting columns.
		// otherwise - values are different, and loop should stop
		if iLessThanJ || col.Less(j, i) {
			break
		}
	}
	return iLessThanJ
}

// see sort.Interface.
// NOTE: Unsafe for use with recurring columns. Consider use Sort(dataset,..) instead
func (set dataset) Swap(i, j int) {
	for _, col := range set {
		col.Swap(i, j)
	}
}

// see Data.LessOther.
// By default sorts by last column ascending
func (set dataset) LessOther(thisRow int, other Data, otherRow int) bool {
	// if no data - don't trigger any change
	if set == nil || len(set) == 0 || other == nil {
		return false
	}
	data := other.(dataset)
	if len(set) != len(data) {
		panic("Unable to compare mismatching number of columns")
	}
	otherColumn := data.At(len(data) - 1)
	return set.At(len(set)-1).LessOther(thisRow, otherColumn, otherRow)
}

// see Data.Slice. Returns a dataset
func (set dataset) Slice(start, end int) Data {
	res := make(dataset, len(set))
	for i := range set {
		res[i] = set[i].Slice(start, end)
	}
	return res
}

// Append a data (assumed by interface spec to be a Dataset)
func (set dataset) Append(other Data) Data {
	if set == nil {
		return other
	}
	if other == nil {
		return set
	}
	data := other.(dataset)
	if len(set) != len(data) {
		panic("Unable to append mismatching number of columns")
	}

	res := make(dataset, set.Width())
	for i := range set {
		res[i] = set[i].Append(data[i])
	}
	return res
}

// see Data.Duplicate. Returns a dataset
func (set dataset) Duplicate(t int) Data {
	if set == nil || len(set) == 0 {
		return set
	}

	res := make(dataset, set.Width())
	for i := 0; i < set.Width(); i++ {
		res[i] = set[i].Duplicate(t)
	}
	return res
}

// see Data.IsNull
func (set dataset) IsNull(i int) bool {
	panic("runtime error: not nullable")
}

// see Data.MarkNull
func (set dataset) MarkNull(i int) {
	panic("runtime error: not nullable")
}

// see Data.Nulls
func (set dataset) Nulls() []bool {
	panic("runtime error: not nullable")
}

// see Data.Equal
func (set dataset) Equal(other Data) bool {
	panic("runtime error: not comparable")
}

// see Data.Copy
func (set dataset) Copy(from Data, fromRow, toRow int) {
	src := from.(dataset)
	for i, d := range set {
		d.Copy(src.At(i), fromRow, toRow)
	}
}

// see Data.Strings
func (set dataset) Strings() []string {
	var res []string
	for _, col := range set {
		res = append(res, "["+strings.Join(col.Strings(), " ")+"]")
	}
	return res
}
