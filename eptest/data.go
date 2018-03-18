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

	// allow types to not implement Strings() with a proper error message
	defer func() {
		if r := recover(); r != nil {
			require.Contains(t, r.(string), "cannot be cast to strings")
		}
	}()
	data.Strings()
	require.Equal(t, oldLen, data.Len())
	require.Equal(t, dataString, fmt.Sprintf("%+v", data))
}
