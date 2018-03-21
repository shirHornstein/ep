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

// VerifyNullsHandling makes sure all functions handel nulls
func VerifyNullsHandling(t *testing.T, typ ep.Type) {
	data := typ.Data(dataLength)
	nullIdx := 1

	t.Run("TestData_IsNull_NoNulls", func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.False(t, isNull)
	})

	data.MarkNull(nullIdx)

	t.Run("TestData_IsNull_WithNulls", func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.True(t, isNull)
	})

	t.Run("TestData_Equal_WithNulls", func(t *testing.T) {
		isEqual := data.Equal(data)
		require.True(t, isEqual)
	})

	t.Run("TestData_IsNull_WithNulls", func(t *testing.T) {
		size := data.Len()
		require.Equal(t, dataLength, size)
	})

	t.Run("TestData_Swap_WithNulls", func(t *testing.T) {
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(0))
		require.False(t, data.IsNull(nullIdx))
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(nullIdx))
		require.False(t, data.IsNull(0))
	})

	t.Run("TestData_IsLess_WithNulls", func(t *testing.T) {
		isLess := data.Less(0, nullIdx)
		require.True(t, isLess)
		isLess = data.Less(nullIdx, 0)
		require.False(t, isLess)
		isLess = data.Less(nullIdx, nullIdx)
		require.False(t, isLess)
	})

	t.Run("TestData_Type_WithNulls", func(t *testing.T) {
		dataType := data.Type()
		require.Equal(t, typ, dataType)
	})

	t.Run("TestData_Slice_WithNulls", func(t *testing.T) {
		slicedData := data.Slice(0, data.Len()/2)
		require.True(t, slicedData.IsNull(nullIdx))
	})

	t.Run("TestData_Append_WithNulls", func(t *testing.T) {
		appendData := data.Append(data)
		require.True(t, appendData.IsNull(nullIdx))
		require.True(t, appendData.IsNull(nullIdx+dataLength))
	})

	t.Run("TestData_Duplicate_WithNulls", func(t *testing.T) {
		duplicateData := data.Duplicate(3)
		require.True(t, duplicateData.IsNull(nullIdx))
		require.True(t, duplicateData.IsNull(nullIdx+dataLength))
		require.True(t, duplicateData.IsNull(nullIdx+2*dataLength))
		require.False(t, duplicateData.IsNull(2*dataLength))
	})

	t.Run("TestData_Strings_WithNulls", func(t *testing.T) {
		s := data.Strings()
		require.True(t, len(s[nullIdx]) == 0)
	})
}
