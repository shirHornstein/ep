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

	t.Run("TestData_Type_invariant", func(t *testing.T) {
		data.Type()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Len_invariant", func(t *testing.T) {
		data.Len()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Less_invariant", func(t *testing.T) {
		data.Less(0, oldLen/2)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_LessOther_invariant", func(t *testing.T) {
		data.LessOther(data, 0, oldLen/2)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Slice_invariant", func(t *testing.T) {
		data.Slice(0, oldLen/2)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Append_invariant", func(t *testing.T) {
		data.Append(data)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Duplicate_invariant", func(t *testing.T) {
		data.Duplicate(5)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	if _, isDataset := data.(ep.Dataset); !isDataset {
		t.Run("TestData_IsNull_invariant", func(t *testing.T) {
			data.IsNull(0)
			require.Equal(t, oldLen, data.Len())
			require.Equal(t, dataString, fmt.Sprintf("%+v", data))
		})

		t.Run("TestData_Nulls_invariant", func(t *testing.T) {
			data.Nulls()
			require.Equal(t, oldLen, data.Len())
			require.Equal(t, dataString, fmt.Sprintf("%+v", data))
		})

		t.Run("TestData_Equal_invariant", func(t *testing.T) {
			isEqual := data.Equal(data)
			require.True(t, isEqual)
			require.Equal(t, oldLen, data.Len())
			require.Equal(t, dataString, fmt.Sprintf("%+v", data))
			isEqual = data.Equal(nil)
			require.False(t, isEqual)
			require.Equal(t, oldLen, data.Len())
			require.Equal(t, dataString, fmt.Sprintf("%+v", data))
		})
	}

	t.Run("TestData_Strings_invariant", func(t *testing.T) {
		data.Strings()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})
}

// VerifyDataNullsHandling makes sure all functions handle nulls
func VerifyDataNullsHandling(t *testing.T, data ep.Data, expectedNullString string) {
	nullIdx := 1
	dataLength := data.Len()
	typ := data.Type()

	t.Run("TestData_IsNull_withoutNulls", func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.False(t, isNull)
	})

	data.MarkNull(nullIdx)

	t.Run("TestData_IsNull_withNulls", func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.True(t, isNull)
	})

	t.Run("TestData_Type_withNulls", func(t *testing.T) {
		dataType := data.Type()
		require.Equal(t, typ, dataType)
	})

	t.Run("TestData_Len_withNulls", func(t *testing.T) {
		size := data.Len()
		require.Equal(t, dataLength, size)
	})

	t.Run("TestData_Less_withNulls", func(t *testing.T) {
		isLess := data.Less(0, nullIdx)
		require.True(t, isLess)
		isLess = data.Less(nullIdx, 0)
		require.False(t, isLess)
		isLess = data.Less(nullIdx, nullIdx)
		require.False(t, isLess)
	})

	t.Run("TestData_Swap_withNulls", func(t *testing.T) {
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(0))
		require.False(t, data.IsNull(nullIdx))
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(nullIdx))
		require.False(t, data.IsNull(0))
	})

	t.Run("TestData_LessOther_withNulls", func(t *testing.T) {
		isLess := data.LessOther(data, nullIdx, 0)
		require.True(t, isLess)
		isLess = data.LessOther(data, 0, nullIdx)
		require.False(t, isLess)
		isLess = data.LessOther(data, nullIdx, nullIdx)
		require.False(t, isLess)
	})

	t.Run("TestData_Slice_withNulls", func(t *testing.T) {
		slicedData := data.Slice(0, data.Len()/2)
		require.True(t, slicedData.IsNull(nullIdx))
	})

	t.Run("TestData_Append_withNulls", func(t *testing.T) {
		appendedData := data.Append(data)
		require.True(t, appendedData.IsNull(nullIdx))
		require.True(t, appendedData.IsNull(nullIdx+dataLength))
	})

	t.Run("TestData_Duplicate_withNulls", func(t *testing.T) {
		duplicatedData := data.Duplicate(3)
		require.True(t, duplicatedData.IsNull(nullIdx))
		require.True(t, duplicatedData.IsNull(nullIdx+dataLength))
		require.True(t, duplicatedData.IsNull(nullIdx+2*dataLength))
		require.False(t, duplicatedData.IsNull(2*dataLength))
	})

	t.Run("TestData_Nulls_withNulls", func(t *testing.T) {
		nullsIndicators := data.Nulls()
		require.False(t, nullsIndicators[0])
		require.True(t, nullsIndicators[nullIdx])
	})

	t.Run("TestData_Equal_withNulls", func(t *testing.T) {
		isEqual := data.Equal(data)
		require.True(t, isEqual)
	})

	t.Run("TestData_Copy_withNulls", func(t *testing.T) {
		newNullIdx := nullIdx + 2
		require.False(t, data.IsNull(newNullIdx))
		require.NotEqual(t, data.IsNull(nullIdx), data.IsNull(newNullIdx))

		data.Copy(data, nullIdx, newNullIdx)
		require.Equal(t, data.IsNull(nullIdx), data.IsNull(newNullIdx))
	})

	t.Run("TestData_Strings_withNulls", func(t *testing.T) {
		strings := data.Strings()
		require.True(t, strings[nullIdx] == expectedNullString)
	})
}
