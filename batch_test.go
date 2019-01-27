package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBatch(t *testing.T) {
	d0 := ep.NewDataset(integers{})
	d1 := ep.NewDataset(integers{1})
	d2 := ep.NewDataset(integers{2, 3})
	d3 := ep.NewDataset(integers{4, 5, 6})
	d4 := ep.NewDataset(integers{7, 8, 9, 10})
	d5 := ep.NewDataset(integers{11, 12, 13, 14, 15})

	t.Run("Size=1", func(t *testing.T) {
		batch := ep.Batch(1)

		runner := ep.Pipeline(batch, &count{})
		res, err := eptest.Run(runner, d0, d1, d2, d3, d4, d5)
		require.NoError(t, err)
		require.Equal(t, 15, res.Len())
		require.Equal(t, 1, res.Width())

		expectedCounts := []string{
			"1", "1", "1", "1", "1", "1", "1", "1",
			"1", "1", "1", "1", "1", "1", "1",
		}
		require.Equal(t, expectedCounts, res.At(0).Strings())
	})

	t.Run("Size=2", func(t *testing.T) {
		batch := ep.Batch(2)

		runner := ep.Pipeline(batch, &count{})
		res, err := eptest.Run(runner, d0, d1, d2, d3, d4, d5)
		require.NoError(t, err)
		require.Equal(t, 8, res.Len())
		require.Equal(t, 1, res.Width())

		expectedCounts := []string{"2", "2", "2", "2", "2", "2", "2", "1"}
		require.Equal(t, expectedCounts, res.At(0).Strings())
	})

	t.Run("Size=3", func(t *testing.T) {
		batch := ep.Batch(3)

		runner := ep.Pipeline(batch, &count{})
		res, err := eptest.Run(runner, d0, d1, d2, d3, d4, d5)
		require.NoError(t, err)
		require.Equal(t, 5, res.Len())
		require.Equal(t, 1, res.Width())

		expectedCounts := []string{"3", "3", "3", "3", "3"}
		require.Equal(t, expectedCounts, res.At(0).Strings())
	})

	t.Run("Size=4", func(t *testing.T) {
		batch := ep.Batch(4)

		runner := ep.Pipeline(batch, &count{})
		res, err := eptest.Run(runner, d0, d1, d2, d3, d4, d5)
		require.NoError(t, err)
		require.Equal(t, 4, res.Len())
		require.Equal(t, 1, res.Width())

		expectedCounts := []string{"4", "4", "4", "3"}
		require.Equal(t, expectedCounts, res.At(0).Strings())
	})

	t.Run("Size=5", func(t *testing.T) {
		batch := ep.Batch(5)

		runner := ep.Pipeline(batch, &count{})
		res, err := eptest.Run(runner, d0, d1, d2, d3, d4, d5)
		require.NoError(t, err)
		require.Equal(t, 3, res.Len())
		require.Equal(t, 1, res.Width())

		expectedCounts := []string{"5", "5", "5"}
		require.Equal(t, expectedCounts, res.At(0).Strings())
	})
}
