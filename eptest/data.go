package eptest

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

const dataLength = 10

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

// VerifyNullHandling makes sure null marking updates the input data
func VerifyNullHandling(t *testing.T, typ ep.Type) {
	data := typ.Data(dataLength)
	idx := 1
	expectedLen := data.Len()
	expectedType := data.Type()

	//mark Null
	isNull := data.IsNull(idx)
	require.False(t, isNull, "IsNull() test is incorrect")

	data.MarkNull(idx)

	isNull = data.IsNull(idx)
	require.True(t, isNull, "MarkNull() test is incorrect")

	//check all API func after MarkNull
	isEqual := data.Equal(data)
	require.True(t, isEqual, "Equal() test is incorrect")

	size := data.Len()
	require.Equal(t, expectedLen, size, "Len() test is incorrect")

	isLess := data.Less(0, idx)
	require.True(t, isLess, "Less() test: 0 < null is incorrect")
	isLess = data.Less(idx, 0)
	require.False(t, isLess, "Less() test: null < 0 is incorrect")

	dataType := data.Type()
	require.Equal(t, expectedType, dataType, "Type() test is incorrect")

	slicedData := data.Slice(0, size/2)
	require.True(t, slicedData.IsNull(idx), "Slice() test is incorrect")

	appendData := data.Append(data)
	require.True(t, appendData.IsNull(idx), "Append() test is incorrect")
	require.True(t, appendData.IsNull(idx+dataLength), "Append() test is incorrect")

	duplicateData := data.Duplicate(3)
	require.True(t, duplicateData.IsNull(idx), "Duplicate() test is incorrect")
	require.True(t, duplicateData.IsNull(idx+dataLength), "Duplicate() test is incorrect")
	require.True(t, duplicateData.IsNull(idx+2*dataLength), "Duplicate() test is incorrect")

	s := data.Strings()
	require.True(t, len(s[idx]) == 0, "Strings() test is incorrect")
}
