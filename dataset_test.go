package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestDataInterface(t *testing.T) {
	dataset := ep.NewDataset(ep.Null.Data(10), str.Data(10))
	eptest.VerifyDataInterfaceInvariant(t, dataset)
}

func TestDataset_Sort_byLastColAscending(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d3, d2)

	sort.Sort(dataset)

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 3, dataset.Width())

	// by default, sorting done by last column, ascending
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(2)))
	// verify other columns were updated as well
	require.Equal(t, "[bar hello a z world bar foo]", fmt.Sprintf("%+v", dataset.At(0)))
	require.Equal(t, "[d a f g b e c]", fmt.Sprintf("%+v", dataset.At(1)))
}

func TestDataset_LessOther(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})
	dataset := ep.NewDataset(d2, d1)
	other := ep.NewDataset(d2)

	isLess := dataset.LessOther(other, 0, 4)
	require.False(t, isLess)

	isLess = dataset.LessOther(other, 2, 5)
	require.True(t, isLess)

	// equal items should return false in both direction
	isLess = dataset.LessOther(other, 0, 5)
	require.False(t, isLess)
	isLessOpposite := other.LessOther(dataset, 5, 0)
	require.False(t, isLessOpposite)
}
