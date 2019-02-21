package ep

import (
	"github.com/satori/go.uuid"
	"io"
)

const batchSize = 1000

// SortGather returns an exchange Runner that gathers all of its input into a
// single node, ordered by given sorting columns. It assumes input from each
// peer is already sorted by these columns. Similar to Gather, on the main node
// it will gather data from all other nodes, and will produce no output on peers
func SortGather(sortingCols []SortingCol) Runner {
	uid, _ := uuid.NewV4()
	return &exchange{
		UID:         uid.String(),
		Type:        sortGather,
		SortingCols: sortingCols,
	}
}

func (ex *exchange) decodeNextSort() (Dataset, error) {
	var err error
	// first decode call, start with fetching first batch of data from each peer
	if ex.batchesNextIdx == nil {
		ex.batchesNextIdx = make([]int, len(ex.decs))
		ex.batches, err = ex.gatherFirstBatches()
		if err != nil {
			return nil, err
		}
	}

	i := ex.pickSample()
	if i == -1 { // no more data to read
		return nil, io.EOF
	}

	// init pre-allocated res with corresponding types
	res := NewDatasetLike(ex.batches[i], batchSize)
	resNextIdx := 0

	// produce a single sorted dataset using Merge-Sort algorithm, assuming we have one
	// sorted batch from each peer by repeating the following:
	// 1. compare all next rows from each batch to find next one according SortingCols
	// 2. add that row to result set
	//    2.1. if entire batch from peer i-th was consumed, fetch next batch from that peer
	// 3. stop when enough data was sorted into result set or no more batches to consume
	for {
		i = ex.pickNext()
		if i == -1 { // no more data to read
			if resNextIdx > 0 {
				return res.Slice(0, resNextIdx).(Dataset), nil
			}
			return nil, io.EOF
		}

		res.Copy(ex.batches[i], ex.batchesNextIdx[i], resNextIdx)

		// update exchange internal state before returning results
		ex.batchesNextIdx[i]++
		if ex.batchesNextIdx[i] == ex.batches[i].Len() {
			// consumed entire i-th batch. fetch next one from i-th peer
			ex.batchesNextIdx[i] = 0
			ex.batches[i], err = ex.decodeFrom(i)
			if err != nil {
				ex.batchesNextIdx[i] = -1 // mark as done
				if err != io.EOF {
					return nil, err
				}
			}
		}

		resNextIdx++
		if resNextIdx == batchSize {
			return res, nil
		}
	}
}

func (ex *exchange) gatherFirstBatches() ([]Dataset, error) {
	var finalErr, err error
	batches := make([]Dataset, len(ex.decs))
	for i := 0; i < len(ex.decs); i++ {
		batches[i], err = ex.decodeFrom(i)
		if err != nil {
			ex.batchesNextIdx[i] = -1
			if err != io.EOF {
				finalErr = err
			}
		}
	}
	return batches, finalErr
}

func (ex *exchange) pickSample() int {
	for i, idx := range ex.batchesNextIdx {
		if idx > -1 {
			return i
		}
	}
	return -1
}

func (ex *exchange) pickNext() int {
	next := -1
	for i := range ex.batches {
		if ex.batchesNextIdx[i] == -1 {
			continue
		}
		if next == -1 || ex.isFirstLess(i, next) {
			next = i
		}
	}
	return next
}

// compare next rows in i-th and next-th batches. Uses pre-defined sorting columns
func (ex *exchange) isFirstLess(i, j int) bool {
	batchI, batchJ := ex.batches[i], ex.batches[j]
	nextI, nextJ := ex.batchesNextIdx[i], ex.batchesNextIdx[j]

	var iLessThanJ bool
	for _, col := range ex.SortingCols {
		colI, colJ := batchI.At(col.Index), batchJ.At(col.Index)

		iLessThanJ = colI.LessOther(nextI, colJ, nextJ) != col.Desc
		// iLessThanJ will be false also for equal values.
		// if LessOther(i, j) and LessOther(j, i) are both false, values are
		// equal. Therefore keep checking next sorting columns.
		// otherwise - values are different, and loop should stop
		if iLessThanJ || colJ.LessOther(nextJ, colI, nextI) != col.Desc {
			break
		}
	}
	return iLessThanJ
}
