package ep

import (
	"sort"
)

// SortingCol defines single sorting condition, composed of col's index and
// sort direction (asc/desc)
type SortingCol struct {
	Index int
	Desc  bool
}

// Sort sorts given dataset by given sorting conditions
func Sort(data Dataset, sortingCols []SortingCol) {
	// if no data - don't change anything
	if data == nil || data.Width() == 0 {
		return
	}
	// if no pre-defined sorting columns - use default sorting
	if len(sortingCols) == 0 {
		sort.Sort(data)
		return
	}

	conditionalSortDataset := newConditionalSortDataset(data.(dataset), sortingCols)
	sort.Sort(conditionalSortDataset)
}

func newConditionalSortDataset(set dataset, sortingCols []SortingCol) *conditionalSortDataset {
	// in case dataset contains recurring columns - find unique columns indices to
	// avoid multiple swapping during sort
	var uniqueColumns []Data
	for i, col := range set {
		unique := set[i].Type().Name() != dummy.Name() // dummies are non unique
		for j := 0; j < i && unique; j++ {
			// use shallow comparison in case dataset contains two different columns with the same data
			if set[i].Equal(set[j]) {
				unique = false
			}
		}
		if unique {
			uniqueColumns = append(uniqueColumns, col)
		}
	}
	sortingInterfaces := make([]sort.Interface, len(sortingCols))
	for i, col := range sortingCols {
		// add new sort interface for col.index-th column
		sortingInterfaces[i] = set.At(col.Index)
		if col.Desc {
			sortingInterfaces[i] = sort.Reverse(sortingInterfaces[i])
		}
	}
	return &conditionalSortDataset{uniqueColumns, sortingInterfaces}
}

type conditionalSortDataset struct {
	uniqueColumns     []Data
	sortingInterfaces []sort.Interface
}

// see sort.Interface. Uses pre-defined sorting columns
func (set *conditionalSortDataset) Less(i, j int) bool {
	var iLessThanJ bool
	for _, col := range set.sortingInterfaces {
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

// see sort.Interface
func (set *conditionalSortDataset) Swap(i, j int) {
	for _, col := range set.uniqueColumns {
		col.Swap(i, j)
	}
}

// see sort.Interface
func (set *conditionalSortDataset) Len() int {
	return set.uniqueColumns[0].Len()
}
