package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func ExampleDataset_Duplicate() {
	dataset1 := ep.NewDataset(strs([]string{"hello", "world"}))
	dataset2 := dataset1.Duplicate(1).(ep.Dataset)
	d2 := dataset2.At(0).(strs)
	d2[0] = "foo"
	d2[1] = "bar"

	fmt.Println(dataset2.At(0)) // copy was modified
	fmt.Println(dataset1.At(0)) // original left intact

	// Output:
	// [foo bar]
	// [hello world]
}

func TestDataInterface(t *testing.T) {
	dataset := ep.NewDataset(ep.Null.Data(10), str.Data(10))
	eptest.VerifyDataInterfaceInvariant(t, dataset)
}

func TestDataset_Sort_byColsAscending(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := ep.NewDataset(d2, d3, d1)

	sort.Sort(dataset)

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 3, dataset.Width())

	// by default, sorting done by last column, ascending
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(0)))
	// verify other columns were updated as well
	require.Equal(t, "[d a f g b e c]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[bar hello a z world bar foo]", fmt.Sprintf("%+v", dataset.At(2)))
}

func TestDataset_LessOther(t *testing.T) {
	var d1 ep.Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 ep.Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})
	dataset := ep.NewDataset(d2, d1)
	other := ep.NewDataset(d2, d2)

	isLess := dataset.LessOther(4, other, 0)
	require.False(t, isLess)

	isLess = dataset.LessOther(5, other, 2)
	require.True(t, isLess)

	// equal items should return false in both direction
	isLess = dataset.LessOther(5, other, 0)
	require.False(t, isLess)
	isLessOpposite := other.LessOther(0, dataset, 5)
	require.False(t, isLessOpposite)
}
