package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExampleDataBuilder() {
	data1 := ep.NewDataset(
		integers{1, 2, 3},
		strs{"a", "b", "c"},
	)
	data2 := ep.NewDataset(
		integers{4, 5, 6, 7},
		strs{"d", "e", "f", "g"},
	)

	builder := ep.NewDatasetBuilder()
	builder.Append(data1)
	builder.Append(data2)
	fmt.Println(builder.Data().Strings())

	// Output:
	// [(1,a) (2,b) (3,c) (4,d) (5,e) (6,f) (7,g)]
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
