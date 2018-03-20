package eptest

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

// VerifyDataInterfaceInvariant makes sure all functions (except Swap())
// does not modify input data, but creating a modified copy when needed
func VerifyDataInterfaceInvariant(t *testing.T, data ep.Data) {
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

	if _, isDataset := data.(ep.Dataset); !isDataset {
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
func VerifyDataMarkNull(t *testing.T, data ep.Data) {
	if _, isDataset := data.(ep.Dataset); !isDataset {
		idx := 1
		require.False(t, data.IsNull(idx))
		data.MarkNull(idx)
		require.True(t, data.IsNull(idx))
	}
}

// VerifyStringFuncWithNullData makes sure null marking updates the input data
// & String() returns empty string for nulls
func VerifyStringFuncWithNullData(t *testing.T, data ep.Data) {
	if _, isDataset := data.(ep.Dataset); !isDataset {
		idx := 1
		require.False(t, data.IsNull(idx))
		data.MarkNull(idx)
		require.True(t, data.IsNull(idx))
		s := data.Strings()
		require.True(t, len(s[idx]) == 0)
	}
}
