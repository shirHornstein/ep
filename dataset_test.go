package ep

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestDataInterface(t *testing.T) {
	dataset := NewDataset(Null.Data(10), str.Data(10)).GetSortable()
	VerifyDataInvariant(t, dataset)
}

func TestDataset_Sort(t *testing.T) {
	var d1 Data = strs([]string{"hello", "world", "foo", "bar"})
	var d2 Data = strs([]string{"1", "2", "4", "3"})

	dataset := newSortableDataset(dataset{d1, d2, d1, d2})
	sort.Sort(dataset)

	require.Equal(t, 4, dataset.Len())
	require.Equal(t, 4, dataset.Width())

	// sorting always done according to last column
	require.Equal(t, "[1 2 3 4]", fmt.Sprintf("%+v", dataset.At(dataset.Width()-1)))
	// verify other columns were updated as well
	require.Equal(t, "[hello world bar foo]", fmt.Sprintf("%+v", dataset.At(0)))
	require.Equal(t, "[1 2 3 4]", fmt.Sprintf("%+v", dataset.At(1)))
}
