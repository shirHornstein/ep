package ep

import (
    "net"
    "time"
)

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {

    // Distribute a Runner to Run on multiple nodes, each is denoted by an
    // address string. The address string doesn't have to be a valid network
    // address - it can be a node name or anything else that the Transport can
    // accept in order to open connections
    Distribute(Runner, addrs ...string) Runner

    // Start listening for incoming Runners to run
    Start() error

    // Stop listening for incoming Runners to run, and close all open
    // connections
    Close() error
}

// NewDistributer creates a Distributer that can be used to distribute work of
// Runners across multiple nodes in a cluster. Distributer must be started on
// all node peers in order for them to receive work.
func NewDistributer(transport Transport) Distributer {
    return &distributer{transport}
}

type distributer struct {
    local Node
    transport Transport
}

func (d *distributer) Distribute(runner Runner, addrs ...string) Runner {
    d := &distributer{d, runner, nodes}
    return Pipeline(d, Gather())
}

func (d *distribute) Run(ctx context.Context, inp, out chan Dataset) error {
    for _, n := range d.nodes {
        conn, err := d.transport.DialTimeout(n.Address(), 50 * time.Millisecond)
        if err != nil {
            return err
        }

        enc := gob.NewEncoder(conn)
        err = enc.Encode(d.runner)
        if err != nil {
            return err
        }
    }

    return d.runner.Run(inp, out)
}

func (d *distribute) Serve(conn net.Conn) error {
    // listen for an incoming runner
    var r Runner
    dec := gob.NewDecoder(conn)
    err := dec.Decode(r)
    if err != nil {
        return err
    }

    ctx := r.(withContext).Context()
    ctx = context.WithValue(ctx, "ep.ThisNode", d.local)

    // wait for the context to arrive
    return r.Run(ctx, inp, out)
}

func (d *distributer) Start() error {
    for _, conn := range d.transport.StreamCh() {
        go d.Serve()
    }
}

func (d *distribute) Close() error {
    return d.transport.Close()
}


// Transport provides an API to a network transport. It's used to open and
// listen for connections that are handled by the Distributer to allocate and
// synchronize Runners across multiple nodes in a cluster
type Transport interface {

    // Transport is a listener, able to provide the Distributer with incoming
    // connections. NOTE that the Address() function is used to denote the
    // current running node. It can be any string, but will be matched against
    // the addresses provided to the Distributer to know which node is the
    // currently running now
    net.Listener

    // Dial should open a connection to a remote address. The address will be
    // the string address provided to the Distributer.
    Dial(addr string) (net.Conn, error)
}
