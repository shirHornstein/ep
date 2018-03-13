package ep

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// VerifyDataInvariant makes sure all functions (except Swap & MarkNull)
// does not modify input data, but creating a modified copy when needed
func VerifyDataInvariant(t *testing.T, data Data) {
	oldLen := data.Len()
	dataString := fmt.Sprintf("%+v", data)

	data.Len()
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	data.Less(0, oldLen/2)
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	data.Type()
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	data.Slice(0, oldLen/2)
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	data.Append(data)
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	data.Duplicate(5)
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))

	if _, isDataset := data.(dataset); !isDataset {
		data.IsNull(0)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))

		isEqual := data.Equal(data)
		require.True(t, isEqual)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
		isEqual = data.Equal(nil)
		require.False(t, isEqual)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	}

	data.Strings()
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))
}

// VerifyDataMarkNull makes sure null marking updates the input data
func VerifyDataMarkNull(t *testing.T, data Data) {
	if _, isDataset := data.(dataset); !isDataset {
		idx := 1
		require.False(t, data.IsNull(idx))
		data.MarkNull(idx)
		require.True(t, data.IsNull(idx))
	}
}
