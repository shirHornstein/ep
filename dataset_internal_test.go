package ep

import (
	"github.com/panoplyio/ep/compare"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllMergeOptionResults(t *testing.T) {
	testCases := []struct {
		left     compare.Result
		right    compare.Result
		expected compare.Result
	}{
		{compare.Equal, compare.Equal, compare.Equal},
		{compare.Equal, compare.BothNulls, compare.BothNulls},
		{compare.Equal, compare.Null, compare.Null},
		{compare.Equal, compare.Greater, compare.Greater},
		{compare.Equal, compare.Less, compare.Less},
		{compare.BothNulls, compare.Equal, compare.BothNulls},
		{compare.BothNulls, compare.BothNulls, compare.BothNulls},
		{compare.BothNulls, compare.Null, compare.Null},
		{compare.BothNulls, compare.Greater, compare.Greater},
		{compare.BothNulls, compare.Less, compare.Less},
		{compare.Null, compare.Equal, compare.Null},
		{compare.Null, compare.BothNulls, compare.Null},
		{compare.Null, compare.Null, compare.Null},
		{compare.Null, compare.Greater, compare.Null},
		{compare.Null, compare.Less, compare.Null},
		{compare.Greater, compare.Equal, compare.Greater},
		{compare.Greater, compare.BothNulls, compare.Greater},
		{compare.Greater, compare.Null, compare.Greater},
		{compare.Greater, compare.Greater, compare.Greater},
		{compare.Greater, compare.Less, compare.Greater},
		{compare.Less, compare.Equal, compare.Less},
		{compare.Less, compare.BothNulls, compare.Less},
		{compare.Less, compare.Null, compare.Less},
		{compare.Less, compare.Greater, compare.Less},
		{compare.Less, compare.Less, compare.Less},
	}

	t.Run("AllMergeOptionResults", func(t *testing.T) {
		var res, mergeWith []compare.Result
		for _, v := range testCases {
			res = []compare.Result{v.left}
			mergeWith = []compare.Result{v.right}
			expected := v.expected
			merge(res, mergeWith)
			require.EqualValues(t, expected, res[0])
		}
	})
}
