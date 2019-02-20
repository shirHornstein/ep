package eptest

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/compare"
	"github.com/stretchr/testify/require"
	"testing"
)

// VerifyDataInterfaceInvariant makes sure all functions (except Swap())
// does not modify input data, but creating a modified copy when needed
func VerifyDataInterfaceInvariant(t *testing.T, data ep.Data) {
	oldLen := data.Len()
	dataString := fmt.Sprintf("%+v", data)

	t.Run("TestData_Type_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Type()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Len_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Len()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Less_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Less(0, oldLen/2)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_LessOther_invariant_"+data.Type().String(), func(t *testing.T) {
		data.LessOther(oldLen/2, data, 0)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Slice_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Slice(0, oldLen/2)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Append_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Append(data)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Duplicate_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Duplicate(5)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_IsNull_invariant_"+data.Type().String(), func(t *testing.T) {
		data.IsNull(0)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Nulls_invariant_"+data.Type().String(), func(t *testing.T) {
		data.Nulls()
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Equal_invariant_"+data.Type().String(), func(t *testing.T) {
		isEqual := data.Equal(data)
		require.True(t, isEqual)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
		isEqual = data.Equal(nil)
		require.False(t, isEqual)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Compare_invariant_"+data.Type().String(), func(t *testing.T) {
		_, err := data.Compare(data)
		require.NoError(t, err)
		require.Equal(t, oldLen, data.Len())
		require.Equal(t, dataString, fmt.Sprintf("%+v", data))
	})

	t.Run("TestData_Strings_invariant_"+data.Type().String(), func(t *testing.T) {
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

	t.Run("TestData_IsNull_withoutNulls_"+data.Type().String(), func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.False(t, isNull)
	})

	data.MarkNull(nullIdx)

	t.Run("TestData_IsNull_withNulls_"+data.Type().String(), func(t *testing.T) {
		isNull := data.IsNull(nullIdx)
		require.True(t, isNull)
	})

	t.Run("TestData_Type_withNulls_"+data.Type().String(), func(t *testing.T) {
		dataType := data.Type()
		require.Equal(t, typ, dataType)
	})

	t.Run("TestData_Len_withNulls_"+data.Type().String(), func(t *testing.T) {
		size := data.Len()
		require.Equal(t, dataLength, size)
	})

	t.Run("TestData_Less_withNulls_"+data.Type().String(), func(t *testing.T) {
		isLess := data.Less(0, nullIdx)
		require.True(t, isLess)
		isLess = data.Less(nullIdx, 0)
		require.False(t, isLess)
		isLess = data.Less(nullIdx, nullIdx)
		require.False(t, isLess)
	})

	t.Run("TestData_Swap_withNulls_"+data.Type().String(), func(t *testing.T) {
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(0))
		require.False(t, data.IsNull(nullIdx))
		data.Swap(0, nullIdx)
		require.True(t, data.IsNull(nullIdx))
		require.False(t, data.IsNull(0))
	})

	t.Run("TestData_LessOther_withNulls_"+data.Type().String(), func(t *testing.T) {
		isLess := data.LessOther(0, data, nullIdx)
		require.True(t, isLess)
		isLess = data.LessOther(nullIdx, data, 0)
		require.False(t, isLess)
		isLess = data.LessOther(nullIdx, data, nullIdx)
		require.False(t, isLess)
	})

	t.Run("TestData_Slice_withNulls_"+data.Type().String(), func(t *testing.T) {
		slicedData := data.Slice(0, data.Len()/2)
		require.True(t, slicedData.IsNull(nullIdx))
	})

	t.Run("TestData_Append_withNulls_"+data.Type().String(), func(t *testing.T) {
		appendedData := data.Append(data)
		require.True(t, appendedData.IsNull(nullIdx))
		require.True(t, appendedData.IsNull(nullIdx+dataLength))
	})

	t.Run("TestData_Duplicate_withNulls_"+data.Type().String(), func(t *testing.T) {
		duplicatedData := data.Duplicate(3)
		require.True(t, duplicatedData.IsNull(nullIdx))
		require.True(t, duplicatedData.IsNull(nullIdx+dataLength))
		require.True(t, duplicatedData.IsNull(nullIdx+2*dataLength))
		require.False(t, duplicatedData.IsNull(2*dataLength))
	})

	t.Run("TestData_Nulls_withNulls_"+data.Type().String(), func(t *testing.T) {
		nullsIndicators := data.Nulls()
		require.False(t, nullsIndicators[0])
		require.True(t, nullsIndicators[nullIdx])
	})

	t.Run("TestData_Equal_withNulls_"+data.Type().String(), func(t *testing.T) {
		isEqual := data.Equal(data)
		require.True(t, isEqual)
	})

	t.Run("TestData_Compare_withNulls_"+data.Type().String(), func(t *testing.T) {
		res, err := data.Compare(data)
		require.NoError(t, err)
		require.Equal(t, compare.BothNulls, res[nullIdx])
	})

	t.Run("TestData_Copy_withNulls_"+data.Type().String(), func(t *testing.T) {
		newNullIdx := nullIdx + 2
		require.False(t, data.IsNull(newNullIdx))
		require.NotEqual(t, data.IsNull(nullIdx), data.IsNull(newNullIdx))

		data.Copy(data, nullIdx, newNullIdx)
		require.Equal(t, data.IsNull(nullIdx), data.IsNull(newNullIdx))
	})

	t.Run("TestData_Strings_withNulls_"+data.Type().String(), func(t *testing.T) {
		strings := data.Strings()
		require.Equal(t, expectedNullString, strings[nullIdx])
	})
}

// VerifyDataBuilder makes sure DataBuilder works on a given type
func VerifyDataBuilder(t *testing.T, data ep.Data) {
	typee := data.Type()
	data.MarkNull(0)

	t.Run("single append", func(t *testing.T) {
		builder := typee.Builder()

		builder.Append(data)
		res := builder.Data()
		require.Equal(t, typee, res.Type())
		require.Equal(t, data.Strings(), res.Strings())
		require.True(t, res.IsNull(0))
	})

	t.Run("multiple appends", func(t *testing.T) {
		builder := typee.Builder()

		builder.Append(data)
		builder.Append(data)
		res := builder.Data()
		require.Equal(t, typee, res.Type())
		require.Equal(t, data.Len()*2, res.Len())
		require.True(t, res.IsNull(0))
		require.True(t, res.IsNull(data.Len()))
		require.Equal(t, append(data.Strings(), data.Strings()...), res.Strings())
	})
}
