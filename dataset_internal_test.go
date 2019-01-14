package ep

import (
	"github.com/panoplyio/ep/compare"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergeEqual(t *testing.T) {
	t.Run("left is Equal", func(t *testing.T) {
		left := []compare.Result{compare.Equal, compare.Equal, compare.Equal, compare.Equal, compare.Equal}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		// Equal should always be overridden
		expected := append([]compare.Result{}, right...)

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}

func TestMergeBothNulls(t *testing.T) {
	t.Run("left is BothNulls", func(t *testing.T) {
		left := []compare.Result{compare.BothNulls, compare.BothNulls, compare.BothNulls, compare.BothNulls, compare.BothNulls}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		expected := []compare.Result{compare.BothNulls, compare.BothNulls, compare.Null, compare.Greater, compare.Less}

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}

func TestMergeNull(t *testing.T) {
	t.Run("left is Null", func(t *testing.T) {
		left := []compare.Result{compare.Null, compare.Null, compare.Null, compare.Null, compare.Null}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		// Null should never be overridden
		expected := append([]compare.Result{}, left...)

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}

func TestMergeGreater(t *testing.T) {
	t.Run("left is Greater", func(t *testing.T) {
		left := []compare.Result{compare.Greater, compare.Greater, compare.Greater, compare.Greater, compare.Greater}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		// Greater should never be overridden
		expected := append([]compare.Result{}, left...)

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}
func TestMergeLess(t *testing.T) {
	t.Run("left is Less", func(t *testing.T) {
		left := []compare.Result{compare.Less, compare.Less, compare.Less, compare.Less, compare.Less}
		right := []compare.Result{compare.Equal, compare.BothNulls, compare.Null, compare.Greater, compare.Less}
		// Less should never be overridden
		expected := append([]compare.Result{}, left...)

		res := left
		mergeWith := right
		merge(res, mergeWith)
		require.Equal(t, expected, res)
	})
}
