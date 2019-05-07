package ep

import (
	"fmt"
	"github.com/panoplyio/ep/compare"
)

var _ = registerGob(Record, NewDataset())
var _ = Types.register("record", Record)

var errMismatch = fmt.Errorf("mismatched number of rows")

// ErrMismatchTypes is a result of an operation on incompatible types
var ErrMismatchTypes = fmt.Errorf("mismatched types")
var errCompareDiffTypes = fmt.Errorf("cannot compare record types with different number of columns")

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

// Record is exposed data type similar to postgres' record, implements Type
var Record = &datasetType{}

type datasetType struct{}

func (sett *datasetType) String() string  { return sett.Name() }
func (*datasetType) Name() string         { return "record" }
func (*datasetType) Size() uint           { panic("call Size on each Data") }
func (sett *datasetType) Data(n int) Data { panic("use NewDataset function") }
func (*datasetType) Builder() DataBuilder { return NewDatasetBuilder() }

type dataset []Data

// NewDataset creates a new Data object that's a horizontal composition of the
// provided Data objects
func NewDataset(data ...Data) Dataset {
	return dataset(data)
}

// NewDatasetLike creates a new Data object at the provided size that has the
// same types as the provided sample dataset
func NewDatasetLike(sample Dataset, size int) Dataset {
	if sample == nil {
		return NewDataset()
	}
	res := make([]Data, sample.Width())
	for i := range res {
		col := sample.At(i)
		if col.Type().Name() == Record.Name() {
			res[i] = NewDatasetLike(col.(Dataset), size)
		} else {
			res[i] = col.Type().Data(size)
		}
	}
	return NewDataset(res...)
}

// NewDatasetTypes creates a new Data object at the provided size and types
func NewDatasetTypes(types []Type, size int) Dataset {
	res := make([]Data, len(types))
	for i, t := range types {
		if t.Name() == Record.Name() {
			panic("NewDatasetTypes invalid call - only concrete types are allowed")
		}
		res[i] = t.Data(size)
	}
	return NewDataset(res...)
}

// NewDatasetBuilder returns a special DataBuilder that allows to efficiently
// append multiple Datasets together.
func NewDatasetBuilder() DataBuilder {
	return &datasetBuilder{}
}

type datasetBuilder struct {
	cols []DataBuilder
}

func (b *datasetBuilder) Append(data Data) {
	ds := data.(Dataset)
	if len(b.cols) == 0 {
		b.cols = make([]DataBuilder, ds.Width())
		for i := range b.cols {
			typee := ds.At(i).Type()
			b.cols[i] = typee.Builder()
		}
	}

	for i, b := range b.cols {
		b.Append(ds.At(i))
	}
}

func (b *datasetBuilder) Data() Data {
	cols := make([]Data, len(b.cols))
	for i, b := range b.cols {
		cols[i] = b.Data()
	}
	return NewDataset(cols...)
}

// Width of the dataset (number of columns)
func (set dataset) Width() int {
	if set == nil {
		return 0
	}
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
	thisLen := set.Len()
	otherLen := other.Len()
	isAnyVariadicNulls := thisLen < 0 || otherLen < 0
	if thisLen != otherLen && !isAnyVariadicNulls {
		return nil, errMismatch
	}
	otherCols := other.(dataset)
	newDataset := make(dataset, 0, len(set)+len(otherCols))
	newDataset = append(newDataset, set...)
	newDataset = append(newDataset, otherCols...)
	return newDataset, nil
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
// NOTE: Unsafe for use with recurring columns. Consider use Sort(dataset, sortingCols) instead
func (set dataset) Less(i, j int) bool {
	var iLessThanJ bool
	for _, col := range set {
		iLessThanJ = col.Less(i, j)
		// iLessThanJ will be false also for equal values.
		// if Less(i, j) and Less(j, i) are both false, values are equal. Therefore
		// keep checking next sorting columns.
		// otherwise - values are different, and loop should stop
		if iLessThanJ || col.Less(j, i) {
			break
		}
	}
	return iLessThanJ
}

// see sort.Interface.
// NOTE: Unsafe for use with recurring columns. Consider use Sort(dataset, sortingCols) instead
func (set dataset) Swap(i, j int) {
	for _, col := range set {
		col.Swap(i, j)
	}
}

// see Data.LessOther.
// By default sorts by columns appearance, ascending
func (set dataset) LessOther(thisRow int, other Data, otherRow int) bool {
	// if no data - don't trigger any change
	if set == nil || len(set) == 0 || other == nil {
		return false
	}
	data := other.(dataset)
	if len(set) != len(data) {
		panic("Unable to compare mismatching number of columns")
	}
	var iLessThanJ bool
	for i, col := range set {
		iLessThanJ = col.LessOther(thisRow, data.At(i), otherRow)
		// iLessThanJ will be false also for equal values.
		// if Less(i, j) and Less(j, i) are both false, values are equal. Therefore
		// keep checking next sorting columns.
		// otherwise - values are different, and loop should stop
		if iLessThanJ || data.At(i).LessOther(otherRow, col, thisRow) {
			break
		}
	}
	return iLessThanJ
}

// see Data.Slice. Returns a dataset
func (set dataset) Slice(start, end int) Data {
	res := make(dataset, len(set))
	for i := range set {
		res[i] = set[i].Slice(start, end)
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
	// i-th row considered to be null if it contains only nulls
	for _, d := range set {
		if !d.IsNull(i) {
			return false
		}
	}
	return true
}

// see Data.MarkNull
func (set dataset) MarkNull(i int) {
	for _, d := range set {
		d.MarkNull(i)
	}
}

// see Data.Nulls
func (set dataset) Nulls() []bool {
	res := make([]bool, set.Len())
	for i := range res {
		res[i] = set.IsNull(i)
	}
	return res
}

// see Data.Equal
func (set dataset) Equal(other Data) bool {
	data, ok := other.(dataset)
	if !ok {
		return false
	}

	for i, d := range set {
		if !d.Equal(data.At(i)) {
			return false
		}
	}
	return true
}

// see Data.Compare
// Compare set and the otherSet, column by column, and calculate final result
// for each row by merging columns comparison results
func (set dataset) Compare(other Data) ([]compare.Result, error) {
	otherSet, ok := other.(Dataset)
	if !ok {
		return nil, ErrMismatchTypes
	}
	if set.Width() != otherSet.Width() {
		return nil, errCompareDiffTypes
	}

	// Prepare first compare results, to be merged with future comparison results
	res, err := set.At(0).Compare(otherSet.At(0))
	if err != nil {
		return nil, err
	}
	for i := 1; i < set.Width(); i++ {
		iterationResult, err := set.At(i).Compare(otherSet.At(i))
		if err != nil {
			return nil, err
		}
		merge(res, iterationResult)
	}
	return res, nil
}

// The merge action follows the rule: if you are not compare.BothNulls and
// not compare.Equal, then the "stronger"'s value
// (compare.Equal < compare.BothNulls < compare.Null < compare.Greater < compare.Less)
// will be assigned to the result.
// Note: the method logic is relevant for dataset Compare func
func merge(res, mergeWith []compare.Result) {
	for i, resI := range res {
		if resI != compare.BothNulls && resI != compare.Equal {
			continue
		}
		if resI < mergeWith[i] {
			res[i] = mergeWith[i]
		}
	}
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
	if set.Len() <= 0 {
		return []string{}
	}
	res := make([]string, set.Len())
	for _, col := range set {
		strs := col.Strings()
		for i, s := range strs {
			res[i] += s + ","
		}
	}
	for i, s := range res {
		res[i] = "(" + s[:len(s)-1] + ")"
	}
	return res
}

func (set dataset) columnStrings() [][]string {
	stringValues := make([][]string, len(set))
	for i, c := range set {
		stringValues[i] = c.Strings()
	}
	return stringValues
}

// ColumnStringsPartial returns string values of selected columns of a provided dataset
func ColumnStringsPartial(set Dataset, cols []int) [][]string {
	stringValues := make([][]string, len(cols))
	for i, col := range cols {
		stringValues[i] = set.At(col).Strings()
	}
	return stringValues
}

// ColumnStrings returns string values of all columns of a provided dataset
func ColumnStrings(set Dataset) [][]string {
	if d, isSimpleDataset := set.(dataset); isSimpleDataset {
		return d.columnStrings()
	}
	width := set.Width()
	stringValues := make([][]string, width)
	for i := 0; i < width; i++ {
		stringValues[i] = set.At(i).Strings()
	}
	return stringValues
}
