package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDatasetSort(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d2, d3, d1, d2, d1)

	ep.Sort(dataset, []ep.SortingCol{{Index: 1, Desc: false}, {Index: 3, Desc: false}})

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 6, dataset.Width())

	// sorting done according to sorting columns
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[bar a hello z world bar foo]", fmt.Sprintf("%+v", dataset.At(3)))
	// verify other columns were updated as well
	require.Equal(t, "[bar a hello z world bar foo]", fmt.Sprintf("%+v", dataset.At(0)))
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(4)))
	require.Equal(t, "[d f a g b e c]", fmt.Sprintf("%+v", dataset.At(2)))
}

func TestDadasetSort_firstDesc(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d2, d3, d2, d1)

	ep.Sort(dataset, []ep.SortingCol{{Index: 1, Desc: true}, {Index: 4, Desc: false}})

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 5, dataset.Width())

	// sorting done according to sorting columns
	require.Equal(t, "[4 3 2 1 1 1 0]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[foo bar world a hello z bar]", fmt.Sprintf("%+v", dataset.At(4)))
	// verify other columns were updated as well
	require.Equal(t, "[foo bar world a hello z bar]", fmt.Sprintf("%+v", dataset.At(0)))
	require.Equal(t, "[4 3 2 1 1 1 0]", fmt.Sprintf("%+v", dataset.At(3)))
	require.Equal(t, "[c e b f a g d]", fmt.Sprintf("%+v", dataset.At(2)))
}

func TestDadasetSort_secondDesc(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d2, d3, d2, d1)

	ep.Sort(dataset, []ep.SortingCol{{Index: 3, Desc: false}, {Index: 0, Desc: true}})

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 5, dataset.Width())

	// sorting done according to sorting columns
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(3)))
	require.Equal(t, "[bar z hello a world bar foo]", fmt.Sprintf("%+v", dataset.At(0)))
	// verify other columns were updated as well
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[d g a f b e c]", fmt.Sprintf("%+v", dataset.At(2)))
	require.Equal(t, "[bar z hello a world bar foo]", fmt.Sprintf("%+v", dataset.At(4)))
}

func TestDadasetSort_severalDesc(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d2, d3, d2, d1)

	ep.Sort(dataset, []ep.SortingCol{{Index: 3, Desc: true}, {Index: 0, Desc: true}})

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 5, dataset.Width())

	// sorting done according to sorting columns
	require.Equal(t, "[4 3 2 1 1 1 0]", fmt.Sprintf("%+v", dataset.At(3)))
	require.Equal(t, "[foo bar world z hello a bar]", fmt.Sprintf("%+v", dataset.At(0)))
	// verify other columns were updated as well
	require.Equal(t, "[4 3 2 1 1 1 0]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[c e b g a f d]", fmt.Sprintf("%+v", dataset.At(2)))
	require.Equal(t, "[foo bar world z hello a bar]", fmt.Sprintf("%+v", dataset.At(4)))
}

func TestDatasetSort_emptyDataset(t *testing.T) {
	require.NotPanics(t, func() {
		ep.Sort(ep.NewDataset(), []ep.SortingCol{{Index: 1, Desc: false}, {Index: 3, Desc: false}})
	})
}

func TestDatasetSort_nilDataset(t *testing.T) {
	require.NotPanics(t, func() {
		ep.Sort(nil, []ep.SortingCol{{Index: 1, Desc: false}, {Index: 3, Desc: false}})
	})
}

func TestDatasetSort_noSortingCols(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "g", "f"})

	dataset := ep.NewDataset(d2, d3, d1)

	require.NotPanics(t, func() {
		ep.Sort(dataset, []ep.SortingCol{})
	})

	// by default sorting done according to columns appearances, ascending
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(0)))
	// verify other columns were updated as well
	require.Equal(t, "[d a f g b e c]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[bar hello z a world bar foo]", fmt.Sprintf("%+v", dataset.At(2)))
}
