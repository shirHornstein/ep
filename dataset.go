package ep

import (
	"fmt"
	"strings"
)

var _ = registerGob(NewDataset(), &datasetType{})

// Dataset is a composite Data interface, containing several internal Data
// objects. It's a Data in itself, but allows traversing and manipulating the
// contained Data intstances
type Dataset interface {
	Data // It's a Data - you can use it anywhere you'd use a Data object

	// Width returns the number of Data instances (columns) in the set
	Width() int

	// At returns the Data instance at index i
	At(i int) Data

	// Expand returns new dataset composed of this dataset's columns and other's
	// columns. Number of rows of both datasets should be equal
	Expand(other Dataset) Dataset

	// Split divides dataset to two parts, where the second part length determined by
	// secondLen argument
	Split(secondLen int) (Dataset, Dataset)

	// GetSortable returns sortable version/extension for this dataset
	GetSortable() Dataset
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
func (set dataset) Expand(other Dataset) Dataset {
	if set == nil {
		return other
	} else if other == nil {
		return set
	}
	if set.Len() != other.Len() {
		panic("Unable to expand mismatching number of rows")
	}

	otherCols := other.(dataset)
	return append(set, otherCols...)
}

// Split returns two datasets, with requested second length
func (set dataset) Split(secondLen int) (Dataset, Dataset) {
	if set.Width() < secondLen {
		panic("Unable to split dataset - not enough columns")
	}
	firstLen := set.Width() - secondLen
	return set[:firstLen], set[firstLen:]
}

// GetSortable returns sortable version/extension for this dataset
func (set dataset) GetSortable() Dataset {
	return newSortableDataset(set)
}

// see Data.Type
func (set dataset) Type() Type {
	return &datasetType{}
}

// Len of the dataset (number of rows). Assumed that all columns are of equal
// length, and thus only checks the first
func (set dataset) Len() int {
	if set == nil || len(set) == 0 {
		return 0
	}

	return set[0].Len()
}

// see sort.Interface. Uses the last column. use sortableDataset.Less instead
func (set dataset) Less(i, j int) bool {
	panic("dataset is not sortable. use sortableDataset type")
}

// see sort.Interface. use sortableDataset.Swap instead
func (set dataset) Swap(i, j int) {
	panic("dataset is not sortable. use sortableDataset type")
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

// see Data.Strings(). Currently not implemented
func (set dataset) Strings() []string {
	var res []string
	for _, d := range set {
		res = append(res, strings.Join(d.Strings(), ","))
	}

	return res
}

type datasetType struct{}

func (sett *datasetType) Name() string         { return "Dataset" }
func (sett *datasetType) String() string       { return sett.Name() }
func (sett *datasetType) Data(n int) Data      { return make(dataset, n) }
func (sett *datasetType) DataEmpty(n int) Data { return make(dataset, 0, n) }

// Sortable version of dataset.
// Note: this type isn't gob-ed on purpose! Only actual dataset should be used
// and sent between peers. Use this version for local sorting, then send original/new
// dataset object
type sortableDataset struct {
	dataset
	uniqueColumns []int
}

// newSortableDataset creates a sortable wrapper of the provided dataset
func newSortableDataset(data dataset) Dataset {
	uniqueColumns := []int{}
	// in case dataset contains recurring columns - find unique columns indices to
	// avoid double Swapping during sort
	for i := range data {
		unique := true
		for j := 0; j < i; j++ {
			// for efficiency - avoid reflection and check address of underlying array
			if fmt.Sprintf("%p", data[i]) == fmt.Sprintf("%p", data[j]) {
				unique = false
			}
		}
		if unique {
			uniqueColumns = append(uniqueColumns, i)
		}
	}
	return sortableDataset{data, uniqueColumns}
}

// see sort.Interface
func (s sortableDataset) Swap(i, j int) {
	for _, idx := range s.uniqueColumns {
		s.At(idx).Swap(i, j)
	}
}

// see sort.Interface. Uses the last column
func (s sortableDataset) Less(i, j int) bool {
	if s.dataset == nil || s.dataset.Width() == 0 {
		return false
	}

	return s.dataset.At(s.dataset.Width()-1).Less(i, j)
}

// Expand returns new dataset with s.dataset and other's columns
func (s sortableDataset) Expand(other Dataset) Dataset {
	expanded := s.dataset.Expand(other).(dataset)
	return newSortableDataset(expanded)
}

// Append a data (assumed by interface spec to be a Dataset)
func (s sortableDataset) Append(data Data) Data {
	dataset := data
	other, isSortable := data.(sortableDataset)
	if isSortable {
		dataset = other.dataset
	}
	return s.dataset.Append(dataset)
}
