package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDataBuilder(t *testing.T) {
	t.Run("one data", func(t *testing.T) {
		db := integer.Builder()
		db.Append(integer.Data(10))
		data := db.Data()
		require.Equal(t, 10, data.Len())
	})

	t.Run("multiple Data objects", func(t *testing.T) {
		db := integer.Builder()
		db.Append(integer.Data(7))
		db.Append(integer.Data(11))
		db.Append(integer.Data(13))
		db.Append(integer.Data(17))
		db.Append(integer.Data(19))
		data := db.Data()
		require.Equal(t, 67, data.Len())
	})

	t.Run("one dataset", func(t *testing.T) {
		db := ep.NewDatasetBuilder()
		d := integer.Data(10)
		ds := ep.NewDataset(d, d)
		db.Append(ds)
		data := db.Data()

		dataset, ok := data.(ep.Dataset)
		require.True(t, ok)
		require.Equal(t, 10, dataset.Len())
		require.Equal(t, 2, dataset.Width())
	})

	t.Run("many datasets", func(t *testing.T) {
		db := ep.NewDatasetBuilder()
		d1 := integer.Data(7)
		d2 := integer.Data(11)
		d3 := integer.Data(13)
		d4 := integer.Data(17)
		d5 := integer.Data(19)

		ds1 := ep.NewDataset(d1, d1)
		ds2 := ep.NewDataset(d2, d2)
		ds3 := ep.NewDataset(d3, d3)
		ds4 := ep.NewDataset(d4, d4)
		ds5 := ep.NewDataset(d5, d5)

		db.Append(ds1)
		db.Append(ds2)
		db.Append(ds3)
		db.Append(ds4)
		db.Append(ds5)
		data := db.Data()

		dataset, ok := data.(ep.Dataset)
		require.True(t, ok)
		require.Equal(t, 67, dataset.Len())
		require.Equal(t, 2, dataset.Width())
	})

	t.Run("records", func(t *testing.T) {
		db := ep.NewDatasetBuilder()
		d1 := integer.Data(7)
		d2 := integer.Data(11)
		d3 := integer.Data(13)
		d4 := integer.Data(17)
		d5 := integer.Data(19)

		ds1 := ep.NewDataset(ep.NewDataset(d1), ep.NewDataset(d1))
		ds2 := ep.NewDataset(ep.NewDataset(d2), ep.NewDataset(d2))
		ds3 := ep.NewDataset(ep.NewDataset(d3), ep.NewDataset(d3))
		ds4 := ep.NewDataset(ep.NewDataset(d4), ep.NewDataset(d4))
		ds5 := ep.NewDataset(ep.NewDataset(d5), ep.NewDataset(d5))

		db.Append(ds1)
		db.Append(ds2)
		db.Append(ds3)
		db.Append(ds4)
		db.Append(ds5)
		data := db.Data()

		dataset, ok := data.(ep.Dataset)
		require.True(t, ok)
		require.Equal(t, 67, dataset.Len())
		require.Equal(t, 2, dataset.Width())
	})
}

func BenchmarkDataBuilder(b *testing.B) {
	dataInts := make([]ep.Data, 100)
	dataStrs := make([]ep.Data, 100)
	dataBoth := make([]ep.Dataset, 100)
	for i := 0; i < len(dataInts); i++ {
		dataInts[i] = integer.Data(1000)
		dataStrs[i] = str.Data(1000)
		dataBoth[i] = ep.NewDataset(dataInts[i], dataStrs[i])
	}

	b.Run("integer", func(b *testing.B) {
		b.Run("DataBuilder", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db := integer.Builder()
				for j := 0; j < len(dataInts); j++ {
					db.Append(dataInts[j])
				}
				data := db.Data()
				require.Equal(b, 100*1000, data.Len())
			}
		})

		b.Run("Append", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data := dataInts[0].Type().Data(0)
				for j := 0; j < len(dataInts); j++ {
					data = data.Append(dataInts[j])
				}
				require.Equal(b, 100*1000, data.Len())
			}
		})
	})

	b.Run("string", func(b *testing.B) {
		b.Run("DataBuilder", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db := str.Builder()
				for j := 0; j < len(dataStrs); j++ {
					db.Append(dataStrs[j])
				}
				data := db.Data()
				require.Equal(b, 100*1000, data.Len())
			}
		})

		b.Run("Append", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data := dataStrs[0].Type().Data(0)
				for j := 0; j < len(dataStrs); j++ {
					data = data.Append(dataStrs[j])
				}
				require.Equal(b, 100*1000, data.Len())
			}
		})
	})

	b.Run("both", func(b *testing.B) {
		b.Run("DataBuilder", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db := ep.NewDatasetBuilder()
				for j := 0; j < len(dataBoth); j++ {
					db.Append(dataBoth[j])
				}
				data := db.Data()
				require.Equal(b, 100*1000, data.Len())
			}
		})

		b.Run("Append", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var data ep.Data = ep.NewDataset()
				for j := 0; j < len(dataBoth); j++ {
					data = data.Append(dataBoth[j])
				}
				require.Equal(b, 100*1000, data.Len())
			}
		})
	})
}
