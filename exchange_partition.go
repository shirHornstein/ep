package ep

import (
	"fmt"
	"github.com/satori/go.uuid"
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

	// before partitioning data, sort it to generate larger batches
	Sort(data, ex.SortingCols)
	stringValues := ColumnStrings(data, ex.PartitionCols...)

	lastSeenHash := ex.getRowHash(stringValues, 0)
	lastSlicedRow := 0
	dataLen := data.Len()
	for row := 1; row < dataLen; row++ {
		hash := ex.getRowHash(stringValues, row)
		if hash == lastSeenHash {
			continue
		}

		dataToEncode := data.Slice(lastSlicedRow, row)
		err := ex.partitionData(dataToEncode, lastSeenHash)
		if err != nil {
			return err
		}

		lastSeenHash = hash
		lastSlicedRow = row
	}

	// leftover
	dataToEncode := data.Slice(lastSlicedRow, dataLen)
	return ex.partitionData(dataToEncode, lastSeenHash)
}

func (ex *exchange) getRowHash(stringValues [][]string, row int) string {
	var sb strings.Builder
	for col := range ex.SortingCols {
		sb.WriteString(stringValues[col][row])
	}
	return sb.String()
}

func (ex *exchange) partitionData(data Data, hash string) error {
	enc, err := ex.getPartitionEncoder(hash)
	if err != nil {
		return err
	}

	return enc.Encode(&req{data})
}

// getPartitionEncoder uses a hash ring to find a node that should handle
// a provided key. This function returns an encoder that handles data
// transmission to the matched node.
func (ex *exchange) getPartitionEncoder(key string) (encoder, error) {
	endpoint, err := ex.hashRing.Get(key)
	if err != nil {
		return nil, fmt.Errorf("cannot find a target node: %s", err)
	}

	enc, ok := ex.encsByKey[endpoint]
	if !ok {
		return nil, fmt.Errorf("no matching node found")
	}

	return enc, nil
}
