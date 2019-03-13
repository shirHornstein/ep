package ep

import (
	"fmt"
	"github.com/satori/go.uuid"
	"sort"
	"strings"
)

// Partition returns an exchange Runner that routes the data between nodes using
// consistent hashing algorithm. The provided column of an incoming dataset
// will be used to find an appropriate endpoint for this data. Order not guaranteed
func Partition(columns ...int) Runner {
	uid, _ := uuid.NewV4()
	sortCols := make([]SortingCol, len(columns))
	for i := 0; i < len(sortCols); i++ {
		sortCols[i] = SortingCol{Index: columns[i]}
	}

	return &exchange{
		UID:           uid.String(),
		Type:          partition,
		SortingCols:   sortCols,
		PartitionCols: columns,
	}
}

// encodePartition encodes an object to a destination connection selected by partitioning
func (ex *exchange) encodePartition(e interface{}) error {
	data, ok := e.(Dataset)
	if !ok {
		return fmt.Errorf("encodePartition called without a dataset")
	}

	dataWithEndpoints, err := ex.addEndpointsToData(data)
	if err != nil {
		return err
	}
	sort.Sort(dataWithEndpoints)
	dataByEndpoint := ex.groupDataByEndpoint(dataWithEndpoints)

	for endpoint, d := range dataByEndpoint {
		enc, ok := ex.encsByKey[endpoint]
		if !ok {
			return fmt.Errorf("no matching node found")
		}

		err := enc.Encode(&req{d})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ex *exchange) addEndpointsToData(data Dataset) (*dataWithEndpoints, error) {
	dataLen := data.Len()
	stringValues := ColumnStrings(data, ex.PartitionCols...)
	endpoints := make([]string, dataLen)
	for row := 0; row < dataLen; row++ {
		hash := ex.getRowHash(stringValues, row)
		endpoint, err := ex.hashRing.Get(hash)
		if err != nil {
			return nil, err
		}
		endpoints[row] = endpoint
	}
	return &dataWithEndpoints{data, endpoints}, nil
}

func (ex *exchange) getRowHash(stringValues [][]string, row int) string {
	var sb strings.Builder
	for col := range ex.SortingCols {
		sb.WriteString(stringValues[col][row])
	}
	return sb.String()
}

func (ex *exchange) groupDataByEndpoint(d *dataWithEndpoints) map[string]Data {
	builderByEndpoint := make(map[string]DataBuilder)
	lastSeenEndpoint := d.endpoints[0]
	lastSlicedRow := 0
	for row := 1; row <= len(d.endpoints); row++ {
		if row != len(d.endpoints) && lastSeenEndpoint == d.endpoints[row] {
			continue
		}

		data := d.data.Slice(lastSlicedRow, row)
		builder := builderByEndpoint[lastSeenEndpoint]
		if builder == nil {
			builder = NewDatasetBuilder()
			builderByEndpoint[lastSeenEndpoint] = builder
		}

		builder.Append(data)
		lastSlicedRow = row
		if row < len(d.endpoints) {
			lastSeenEndpoint = d.endpoints[row]
		}
	}

	dataByEndpoint := make(map[string]Data)
	for endpoint, builder := range builderByEndpoint {
		dataByEndpoint[endpoint] = builder.Data()
	}

	return dataByEndpoint
}

type dataWithEndpoints struct {
	data      Dataset
	endpoints []string
}

func (s *dataWithEndpoints) Len() int {
	return len(s.endpoints)
}

func (s *dataWithEndpoints) Less(i int, j int) bool {
	return s.endpoints[i] < s.endpoints[j]
}

func (s *dataWithEndpoints) Swap(i int, j int) {
	s.data.Swap(i, j)
	s.endpoints[i], s.endpoints[j] = s.endpoints[j], s.endpoints[i]
}
