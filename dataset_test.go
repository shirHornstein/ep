package ep

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestDataInterface(t *testing.T) {
	dataset := NewDataset(Null.Data(10), str.Data(10))
	VerifyDataInvariant(t, dataset)
}

func TestDataset_Sort_byLastColAscending(t *testing.T) {
	var d1 Data = strs([]string{"hello", "world", "foo", "bar", "bar", "a", "z"})
	var d2 Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d3 Data = strs([]string{"a", "b", "c", "d", "e", "f", "g"})

	dataset := NewDataset(d1, d3, d2)

	sort.Sort(dataset)

	require.Equal(t, 7, dataset.Len())
	require.Equal(t, 3, dataset.Width())

	// sorting done by last column
	require.Equal(t, "[0 1 1 1 2 3 4]", fmt.Sprintf("%+v", dataset.At(2)))
	// verify other columns were updated as well
	require.Equal(t, "[bar hello a z world bar foo]", fmt.Sprintf("%+v", dataset.At(0)))
	require.Equal(t, "[d a f g b e c]", fmt.Sprintf("%+v", dataset.At(1)))
}
