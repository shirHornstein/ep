package ep

import (
    "net"
)

// Transport provides an API to a network transport. It's used to open and
// listen for connections that are handled by the Distributer to allocate and
// synchronize Runners across multiple nodes in a cluster
type Transport interface {
    DialTimeout(addr string, timeout time.Duration) (net.Conn, error)
    StreamCh() <-chan net.Conn
    Shutdown() error
}

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {
    Distribute(Runner, nodes ...Node) (Runner, error)
    Shutdown() error
}

// CreateDistributer creates a new Distributer that can be used to distribute
// work of Runners across multiple nodes in a cluster. It also starts a server
// that listens for incoming connections being fed from the Transport, and thus
// should run on every node that wishes to participate in the work
func CreateDistributer(local Node, transport Transport) Distributer {
    d := &distributer{transport}
    go d.Start()
    return d
}

type distributer struct {
    local Node
    transport Transport
}

func (d *distributer) Start() error {
    for _, conn := range d.transport.StreamCh() {
        go d.Serve()
    }
}

func (d *distribute) Serve(conn net.Conn) error {
    return nil
}
