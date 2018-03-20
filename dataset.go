package ep

import (
	"fmt"
	"strings"
)

var _ = registerGob(NewDataset(), &datasetType{})
var errMismatch = fmt.Errorf("mismatched number of rows")

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
	if set.Len() != other.Len() {
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

// see sort.Interface.
// Len of the dataset (number of rows). Assumed that all columns are of equal
// length, and thus only checks the first
func (set dataset) Len() int {
	if set == nil || len(set) == 0 {
		return 0
	}

	return set[0].Len()
}

// see sort.Interface.
// By default sorts by last column ascending. Consider use Sort(dataset,..) instead
func (set dataset) Less(i, j int) bool {
	// if no data - don't trigger any change
	if set == nil || len(set) == 0 {
		return false
	}
	return set.At(len(set)-1).Less(i, j)
}

// see sort.Interface.
// NOTE: Unsafe for use with recurring columns. Consider use Sort(dataset,..) instead
func (set dataset) Swap(i, j int) {
	for _, col := range set {
		col.Swap(i, j)
	}
}

// see Data.Type
func (set dataset) Type() Type {
	return &datasetType{}
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
func (set dataset) Append(data Data) Data {
	other := data.(dataset)
	if set == nil {
		return other
	} else if other == nil {
		return set
	}

	if len(set) != len(other) {
		panic("Unable to append mismatching number of columns")
	}

	res := make(dataset, set.Width())
	for i := range set {
		res[i] = set[i].Append(other[i])
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

// see Data.Equal
func (set dataset) Equal(data Data) bool {
	panic("runtime error: not comparable")
}

// see Data.Strings
func (set dataset) Strings() []string {
	var res []string
	for _, col := range set {
		res = append(res, "["+strings.Join(col.Strings(), " ")+"]")
	}
	return res
}

type datasetType struct{}

func (sett *datasetType) Name() string         { return "Dataset" }
func (sett *datasetType) String() string       { return sett.Name() }
func (sett *datasetType) Data(n int) Data      { return make(dataset, n) }
func (sett *datasetType) DataEmpty(n int) Data { return make(dataset, 0, n) }
