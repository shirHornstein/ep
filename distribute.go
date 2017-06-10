package ep

// NewScatter returns a distribute Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a roubd-
// robin to the nodes.
func NewScatter() Runner {
    return nil
}

// NewGather returns a distribute Runner that gathers all of its input into a
// single node. In all other nodes it will produce no output, but on the main
// node it will be passthrough from all of the other nodes
func NewGather(to Node) Runner {
    return nil
}

// NewBroadcast returns a distribute Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all of the inputs from
// all nodes (order not guaranteed)
func NewBroadcast() Runner {
    return nil
}

// NewPartition returns a distribute Runner that scatters its input to all other
// nodes, except that a predicate function can be provided that determines the
// target node of each dataset. This is useful for partitioning based on the
// values in the data
func NewPartition(predicate func(Dataset) Node) Runner {
    return nil
}

// NewDistribute creates a generic Runner that distributes all of its input data
// to the provided nodes. This Runner, when executed on in parallel on multiple
// nodes produces different results
func NewDistribute(to []Node) Runner {
    return nil
}
