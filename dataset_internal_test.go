package ep

import (
	"github.com/panoplyio/ep/compare"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllMergeOptionResults(t *testing.T) {
	t.Run("CompareResultEqualMergeOptions", func(t *testing.T) {
		left := []compare.Result{compare.Equal, compare.Equal, compare.Equal, compare.Equal, compare.Equal}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})

	t.Run("CompareResultBothNullsMergeOptions", func(t *testing.T) {
		left := []compare.Result{compare.BothNulls, compare.BothNulls, compare.BothNulls, compare.BothNulls, compare.BothNulls}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.BothNulls, compare.BothNulls, compare.Null, compare.Greater, compare.Less}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})

	t.Run("CompareResultNullMergeOptions", func(t *testing.T) {
		left := []compare.Result{compare.Null, compare.Null, compare.Null, compare.Null, compare.Null}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.Null, compare.Null, compare.Null, compare.Null, compare.Null}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})

	t.Run("CompareResultGreaterMergeOptions", func(t *testing.T) {
		left := []compare.Result{compare.Greater, compare.Greater, compare.Greater, compare.Greater, compare.Greater}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.Greater, compare.Greater, compare.Greater, compare.Greater, compare.Greater}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})

	t.Run("CompareResultLessMergeOptions", func(t *testing.T) {
		left := []compare.Result{compare.Less, compare.Less, compare.Less, compare.Less, compare.Less}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.Less, compare.Less, compare.Less, compare.Less, compare.Less}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}
