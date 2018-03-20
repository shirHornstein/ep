package eptest

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

const DataLength = 10

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
func VerifyNullHandling(t *testing.T, data ep.Data) {
	idx := 1
	expectedLen := data.Len()
	expectedType := data.Type()
	expectedAppendData := data.Type().Data(2)
	expectedDuplicateData := data.Type().Data(5 * DataLength)

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

	slicedData := data.Slice(0, idx)
	require.Equal(t, reflect.ValueOf(data).Elem().FieldByName("Values").Slice(0, idx).String(), reflect.ValueOf(slicedData).Elem().FieldByName("Values").String(), "Slice() test: Values are incorrect")
	require.Equal(t, reflect.ValueOf(data).Elem().FieldByName("NullsIndicators").Slice(0, idx).String(), reflect.ValueOf(slicedData).Elem().FieldByName("NullsIndicators").String(), "Slice() test: NullsIndicators are incorrect")

	appendData := data.Append(data)
	require.Equal(t, reflect.ValueOf(expectedAppendData).Elem().FieldByName("Values").String(), reflect.ValueOf(appendData).Elem().FieldByName("Values").String(), "Append() test: Values are incorrect")
	require.Equal(t, reflect.ValueOf(expectedAppendData).Elem().FieldByName("NullsIndicators").String(), reflect.ValueOf(appendData).Elem().FieldByName("NullsIndicators").String(), "Append() test: NullsIndicators are incorrect")

	duplicateData := data.Duplicate(5)
	require.Equal(t, reflect.ValueOf(expectedDuplicateData).Elem().FieldByName("Values").String(), reflect.ValueOf(duplicateData).Elem().FieldByName("Values").String(), "Duplicate() test: Values are incorrect")
	require.Equal(t, reflect.ValueOf(expectedDuplicateData).Elem().FieldByName("NullsIndicators").String(), reflect.ValueOf(duplicateData).Elem().FieldByName("NullsIndicators").String(), "Duplicate() test: NullsIndicators are incorrect")

	s := data.Strings()
	require.True(t, len(s[idx]) == 0, "Strings() test is incorrect")
}
