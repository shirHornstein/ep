package ep

import (
	"context"
	"sync"
)

var _ = registerGob(&merger{})

// Merge returns an exchange Runner that gathers all of its input into a
// single node, ordered by given sorting columns. It assumes input from
// each peer is already sorted by these columns. In all other nodes it
// will produce no output, but on the main node it will be passthrough
// from all of the other nodes
func Merge(sortingCols []SortingCol) Runner {
	return &merger{
		Gather:      newSelectiveGather(),
		SortingCols: sortingCols,
	}
}

type merger struct {
	Gather      *exchange
	SortingCols []SortingCol

	peersNextIdx []int
}

const batchSize = 1000

func (*merger) Returns() []Type { return []Type{Wildcard} }
func (r *merger) Run(origCtx context.Context, inp, out chan Dataset) (err error) {
	ctx, cancel := context.WithCancel(origCtx)

	wg := sync.WaitGroup{}
	defer wg.Wait() // make sure gather finished as well

	// run gather exchange
	gatherOut := make(chan Dataset)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = r.Gather.Run(ctx, inp, gatherOut)
		close(gatherOut)
		if err != nil {
			cancel()
		}
	}()

	batches := r.receiveFirstBatches(gatherOut)
	var example Dataset
	for _, batch := range batches {
		if batch != nil && batch.Width() > 0 {
			example = batch
			break
		}
	}
	if example == nil {
		// if this node is not a receiver - mark gather that no data is required
		r.Gather.next <- -1
		// no need to drain gatherOut, as no data should be written by non-receiver node
		return
	}

	// init pre-allocated res with corresponding types
	res := r.allocateResBatch(example)
	resNextIdx := 0

	for {
		select {
		case <-origCtx.Done():
			cancel()
			return
		case <-ctx.Done():
			return
		default:
			// perform distributed merge sort by examine all next rows in each batch
			// and pick the first one in the defined order
			i := r.pickNext(batches)
			if i == -1 {
				// mark gather that we done (i.e. all peers returned EOF)
				r.Gather.next <- -1
				if resNextIdx > 0 {
					out <- res.Slice(0, resNextIdx).(Dataset)
				}
				return
			}

			res.Copy(batches[i], r.peersNextIdx[i], resNextIdx)
			resNextIdx++
			if resNextIdx == batchSize {
				out <- res
				// init another pre-allocated res with corresponding types
				res = r.allocateResBatch(res)
				resNextIdx = 0
			}

			r.peersNextIdx[i]++

			// consumed entire i-th batch. fetch next one from i-th peer
			if r.peersNextIdx[i] == batches[i].Len() {
				r.peersNextIdx[i] = 0
				batches[i] = r.receiveNextFrom(i, gatherOut)
				if batches[i] == nil {
					r.peersNextIdx[i] = -1
				}
			}
		}
	}
}

func (r *merger) receiveFirstBatches(gatherOut chan Dataset) []Dataset {
	// first, wait for signal that gather initialization was done
	<-gatherOut

	numOfSources := len(r.Gather.decs)
	batches := make([]Dataset, numOfSources)
	r.peersNextIdx = make([]int, numOfSources)
	for i := 0; i < numOfSources; i++ {
		r.Gather.next <- i
		batches[i] = <-gatherOut
		if batches[i] == nil {
			r.peersNextIdx[i] = -1
		}
	}
	return batches
}

func (r *merger) receiveNextFrom(i int, gatherOut chan Dataset) Dataset {
	r.Gather.next <- i
	return <-gatherOut
}

// allocateResBatch allocates new result array with corresponding types and batchSize rows
func (r *merger) allocateResBatch(example Dataset) Dataset {
	res := make([]Data, example.Width())
	for i := range res {
		col := example.At(i)
		res[i] = col.Type().Data(batchSize)
	}
	return NewDataset(res...)
}

func (r *merger) pickNext(batches []Dataset) int {
	next := -1
	for i := range batches {
		if r.peersNextIdx[i] == -1 {
			continue
		}
		if next == -1 || r.isFirstLess(batches, i, next) {
			next = i
		}
	}
	return next
}

// compare next rows in i-th and next-th batches. Uses pre-defined sorting columns
func (r *merger) isFirstLess(batches []Dataset, i, j int) bool {
	batchI, batchJ := batches[i], batches[j]
	nextI, nextJ := r.peersNextIdx[i], r.peersNextIdx[j]

	var iLessThanJ bool
	for _, col := range r.SortingCols {
		colI, colJ := batchI.At(col.Index), batchJ.At(col.Index)

		iLessThanJ = colI.LessOther(nextI, colJ, nextJ) != col.Desc
		// iLessThanJ will be false also for equal values.
		// if colI.LessOther(colJ) and colJ.LessOther(colI) are both false, values
		// are equal. Therefore leave stop as false and keep checking next sorting
		// columns. otherwise - values are different, and loop should stop
		if iLessThanJ || colJ.LessOther(nextJ, colI, nextI) != col.Desc {
			break
		}
	}
	return iLessThanJ
}
