package ep

import (
	"context"
)

var _ = registerGob(&from{})

// From creates a new runner from the provided Data (and, by extension,
// Datasets) provided. The data types must be equal. The first argument, n,
// determines how many times to output the provided datasets (for benchmarks, for
// example).
//
// NOTE that when distributed, From would split the datasets across the nodes
// in a round-robin fashion.
func From(n int, data ...Data) Runner {
	// turn input data into datasets
	datasets := make([]Dataset, len(data))
	for i, d := range data {
		ds, ok := d.(Dataset)
		if ok {
			datasets[i] = ds
		} else {
			datasets[i] = NewDataset(d)
		}
	}

	return &from{n, datasets}
}

type from struct {
	N        int
	Datasets []Dataset
}

// returns the type of the first dataset. assumes that the dataset types are
// always consistent, yet this is not verified anywhere.
func (r *from) Returns() []Type {
	if len(r.Datasets) == 0 {
		return nil
	}

	data := r.Datasets[0]
	types := make([]Type, data.Width())
	for i := range types {
		types[i] = data.At(i).Type()
	}
	return types
}

func (r *from) Run(ctx context.Context, inp, out chan Dataset) error {
	for range inp {
	} // drain the input

	datasets := r.distribute(ctx)
	for i := 0; i < r.N; i++ {
		for _, data := range datasets {
			out <- data
		}
	}
	return nil
}

// when running in a distributed mode, each node should return its own portion
// of the provided datasets, rather than all of them.
func (r *from) distribute(ctx context.Context) (res []Dataset) {
	allNodes, _ := ctx.Value(allNodesKey).([]string)
	thisNode, _ := ctx.Value(thisNodeKey).(string)

	if len(allNodes) == 0 {
		return r.Datasets // not distributed, return everything.
	}

	for i, data := range r.Datasets {
		// which node should emit this instance?
		node := allNodes[i%len(allNodes)]
		if node == thisNode {
			// only keep the datasets that should be emitted by this node.
			res = append(res, data)
		}
	}

	return res
}
