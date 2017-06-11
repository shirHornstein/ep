package ep

import (
    "context"
)

// NewScatter returns a distribute Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a roubd-
// robin to the nodes.
func NewScatter() Runner {
    return nil
}

// NewGather returns a distribute Runner that gathers all of its input into a
// single node. In all other nodes it will produce no output, but on the main
// node it will be passthrough from all of the other nodes
func NewGather() Runner {
    return nil
}

// NewBroadcast returns a distribute Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all of the inputs from
// all nodes (order not guaranteed)
func NewBroadcast() Runner {
    return nil
}

// NewPartition returns a distribute Runner that scatters its input to all other
// nodes, except that it uses the last Data column in the input datasets to
// determine the target node of each dataset. This is useful for partitioning
// based on the values in the data, thus guaranteeing that all equal values land
// in the same target node
func NewPartition() Runner {
    return nil
}

// distribute is a Runner that exchanges that between peer nodes
type distribute struct {
    UUID string
}

func (d *distribute) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    conns := newConnections(d, ctx)
    defer conns.Close(err) // notify peers that we're done

    // send the local data to the distributed target nodes
    go func() {
        for data := range inp {
            err = conns.Send(data)
            if err != nil {
                return
            }
        }
    }()

    // listen to all nodes for incoming data
    var data Dataset
    for {
        data, err = conns.Receive()
        if err != nil {
            return
        }

        out <- data
    }
}
