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

func TestNewDatasetLike(t *testing.T) {
	recordsColumn := ep.NewDataset(integer.Data(2))
	data := ep.NewDataset(strs([]string{"hello", "world"}), integer.Data(2), recordsColumn)
	data2 := ep.NewDatasetLike(data, 10)

	require.Equal(t, 3, data2.Width())
	require.Equal(t, 10, data2.Len())
	require.Equal(t, str, data2.At(0).Type())
	require.Equal(t, integer, data2.At(1).Type())
	require.Equal(t, "(,0,(0))", data2.Strings()[0])
	require.Equal(t, ep.Record, data2.At(2).Type())
}

func TestNewDatasetTypes(t *testing.T) {
	data := ep.NewDatasetTypes([]ep.Type{str, integer}, 10)

	require.Equal(t, 2, data.Width())
	require.Equal(t, 10, data.Len())
	require.Equal(t, str, data.At(0).Type())
	require.Equal(t, integer, data.At(1).Type())
	require.Equal(t, "(,0)", data.Strings()[0])
}

func TestNewDatasetTypes_panicWithRecords(t *testing.T) {
	require.Panics(t, func() {
		ep.NewDatasetTypes([]ep.Type{str, ep.Record, integer}, 10)
	})
}

func TestDatasetInvariant(t *testing.T) {
	d1 := strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	d2 := strs([]string{"a", "b", "c", "", "e", "f", "g"})
	d3 := integer.Data(7)

	eptest.VerifyDataInterfaceInvariant(t, ep.NewDataset(d1, d2, d3))
}

func TestDataset_sort(t *testing.T) {
	d1 := strs([]string{"1", "2", "1", "0", "3", "1", "1"})
	d2 := strs([]string{"a", "b", "a", "", "e", "z", "g"})
	d3 := strs([]string{"a", "b", "a2", "", "e", "z", "g"})

	dataset := ep.NewDataset(d1, d2, d3)

	sort.Sort(dataset)

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 3, dataset.Width())

	// by default, sorting done by last column, ascending
	require.Equal(t, "[0 1 1 1 1 2 3]", fmt.Sprintf("%+v", dataset.At(0)))
	// verify other columns were updated as well
	require.Equal(t, "[ a a g z b e]", fmt.Sprintf("%+v", dataset.At(1)))
	require.Equal(t, "[ a a2 g z b e]", fmt.Sprintf("%+v", dataset.At(2)))
}

func TestDataset_sortDescending(t *testing.T) {
	expected := []string{"(3,e,e)", "(2,b,b)", "(1,z,z)", "(1,g,g)", "(1,a,a2)", "(1,a,a)", "(0,,)"}
	d1 := strs([]string{"1", "2", "1", "0", "3", "1", "1"})
	d2 := strs([]string{"a", "b", "a", "", "e", "z", "g"})
	d3 := strs([]string{"a", "b", "a2", "", "e", "z", "g"})

	dataset := ep.NewDataset(d1, d2, d3)

	sort.Sort(sort.Reverse(dataset))
	require.Equal(t, expected, dataset.Strings())
}

func TestDataset_LessOther_breakOnFirstColumn(t *testing.T) {
	d1 := strs([]string{"b", "c"})
	d2 := strs([]string{"b", "a"})
	dataset := ep.NewDataset(d1, d1)
	other := ep.NewDataset(d2, d1)

	isLess := dataset.LessOther(0, other, 1) // b < a?
	require.False(t, isLess)
	// same comparison, other direction
	isLess = other.LessOther(1, dataset, 0) // a < b?
	require.True(t, isLess)
}

func TestDataset_LessOther_breakOnSecondColumn(t *testing.T) {
	d1 := strs([]string{"c", "a"})
	d2 := strs([]string{"a", "x"})
	dataset := ep.NewDataset(d1, d1)
	other := ep.NewDataset(d2, d1)

	// dataset row 1: (a,a)
	// other row 0: (a,c)
	// first column equal, compare second column
	isLess := dataset.LessOther(1, other, 0) // (a,a) < (a,c)?
	require.True(t, isLess)
	// same comparison, other direction
	isLess = other.LessOther(0, dataset, 1) // (a,c) < (a,a)?
	require.False(t, isLess)
}

func TestDataset_LessOther_equalRows(t *testing.T) {
	d1 := strs([]string{"a", "a"})
	d2 := strs([]string{"a", "x"})
	dataset := ep.NewDataset(d1, d1)
	other := ep.NewDataset(d2, d1)

	// equal items should return false in both direction
	isLess := dataset.LessOther(1, other, 0) // (a,a) < (a,a)?
	require.False(t, isLess)
	// same comparison, other direction
	isLessOpposite := other.LessOther(0, dataset, 1) // (a,a) < (a,a)?
	require.False(t, isLessOpposite)
}

func TestDataset_Strings(t *testing.T) {
	expected := []string{"(1,a)", "(2,b)", "(4,c)", "(0,)", "(3,e)", "(1,f)", "(1,g)"}
	d1 := strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	d2 := strs([]string{"a", "b", "c", "", "e", "f", "g"})

	dataset := ep.NewDataset(d1, d2)
	require.Equal(t, expected, dataset.Strings())
}

func TestColumnStrings(t *testing.T) {
	d1 := strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	d2 := strs([]string{"a", "b", "c", "", "e", "f", "g"})
	dataset := ep.NewDataset(d1, d2)

	t.Run("AllColumns", func(t *testing.T) {
		require.Equal(t, [][]string{d1, d2}, ep.ColumnStrings(dataset))
	})

	t.Run("FirstColumn", func(t *testing.T) {
		require.Equal(t, [][]string{d1}, ep.ColumnStrings(dataset, 0))
	})

	t.Run("SecondColumn", func(t *testing.T) {
		require.Equal(t, [][]string{d2}, ep.ColumnStrings(dataset, 1))
	})

	t.Run("BothColumns", func(t *testing.T) {
		require.Equal(t, [][]string{d1, d2}, ep.ColumnStrings(dataset, 0, 1))
	})
}
