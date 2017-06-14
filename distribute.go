package ep

import (
    "io"
    "net"
    "context"
    "encoding/gob"
)

const (
    sendGather = 1
    sendScatter = 2
    sendBroadcast = 3
    sendPartition = 4
)

// Scatter returns a distribute Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a round-
// robin to the nodes.
func Scatter() Runner {
    return nil
}

// Gather returns a distribute Runner that gathers all of its input into a
// single node. In all other nodes it will produce no output, but on the main
// node it will be passthrough from all of the other nodes
func Gather() Runner {
    return nil
}

// Broadcast returns a distribute Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all of the inputs from
// all nodes (order not guaranteed)
func Broadcast() Runner {
    return nil
}

// Partition returns a distribute Runner that scatters its input to all other
// nodes, except that it uses the last Data column in the input datasets to
// determine the target node of each dataset. This is useful for partitioning
// based on the values in the data, thus guaranteeing that all equal values land
// in the same target node
func Partition() Runner {
    return nil
}

// distribute is a Runner that exchanges data between peer nodes
type distribute struct {
    Uid string
    SendTo int

    encs []*gob.Encoder // encoders to all destination connections
    decs []*gob.Decoder // decoders from all source connections
    conns []net.Conn // all open connections (used for closing)
    encsNext int // Encoders Round Robin next index
    decsNext int // Decoders Round Robin next index
}

func (d *distribute) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    defer func() { d.Close(err) }()
    err = d.Init(ctx)
    if err != nil {
        return
    }

    // send the local data to the distributed target nodes
    go func() {
        for data := range inp {
            err = d.Send(data)
            if err != nil {
                return
            }
        }
    }()

    // listen to all nodes for incoming data
    var data Dataset
    for {
        data, err = d.Receive()
        if err != nil {
            return
        }

        out <- data
    }
}

// Send a dataset to destination nodes
func (d *distribute) Send(data Dataset) error {
    switch d.sendTo {
    case sendScatter:
            return d.EncodeNext(data)
    default:
        return d.EncodeAll(data)
    }
}

func (d *distribute) Receive() (Dataset, error) {
    data := Dataset{}
    err := d.DecodeNext(data)
    return data, err
}

// Close all open connections. If an error object is supplied, it's first
// encoded to all connections before closing.
func (d *distribute) Close(err error) error {
    var errOut error
    if err != nil {
        errOut = d.EncodeAll(err)
    }

    for _, conn := range d.conns {
        err1 := conn.Close()
        if err1 != nil {
            errOut = err1
        }
    }

    return errOut
}

// Encode an object to all destination connections
func (d *distribute) EncodeAll(e interface{}) (err error) {
    for _, enc := range d.encs {
        err1 := enc.Encode(e)
        if err1 != nil {
            err = err1
        }
    }

    return err
}

// Encode an object to the next destination connection in a round robin
func (d *distribute) EncodeNext(e interface{}) error {
    if len(d.encs) == 0 {
        return io.ErrClosedPipe
    }

    d.encsNext = (d.encsNext + 1) % len(d.encs)
    return d.encs[d.encsNext].Encode(e)
}

// Decode an object from the next source connection in a round robin
func (d *distribute) DecodeNext(e interface{}) error {
    if len(d.decs) == 0 {
        return io.EOF
    }

    i := (d.decsNext + 1) % len(d.decs)
    err := d.decs[i].Decode(e)
    if err == io.EOF {
        // remove the current decoder and try again
        d.decs = append(d.decs[:i], d.decs[i + 1:]...)
        return d.DecodeNext(e)
    } else if err != nil {
        return err
    }

    d.decsNext = i
    return nil
}

// initialize the connections, encoders & decoders
func (d *distribute) Init(ctx context.Context) error {
    var err error
    var isThisTarget bool // is this node also a destination?
    
    allNodes := ctx.Value("ep.AllNodes").([]Node)
    thisNode := ctx.Value("ep.ThisNode").(Node)
    masterNode := ctx.Value("ep.MasterNode").(Node)

    targetNodes = allNodes
    if d.send == sendGather {
        targetNodes = []Node{masterNode}
    }

    // open a connection to all target nodes
    connsMap := map[Node]net.Conn{}
    for _, n := range targetNodes {
        isThisTarget = isThisTarget || n == thisNode // TODO: short-circuit
        conn, err = connect(n, d.Uid)
        if err != nil {
            return nil, err
        }

        connsMap[n] = conn
        d.conns = append(d.conns, conn)
        d.encs = append(d.encs, gob.NewEncoder(conn))
    }

    // if we're also a destination, listen to all nodes
    for i := 0; isThisTarget && i < len(allNodes); i += 1 {
        n := AllNodes[i]

        // if we already established a connection to this node from the targets,
        // re-use it. We don't need 2 uni-directional connections.
        if connsMap[n] != nil {
            d.decs = append(d.decs, gob.NewDecoder(connsMap[n]))
            continue
        }

        conn, err = connect(n, d.Uid)
        if err != nil {
            return
        }

        d.conns = append(d.conns, conn)
        d.decs = append(d.decs, gob.NewDeocder(conn))
    }
}
