package ep

import (
    "net"
)

var _ = registerGob(req{})

// Transport provides an API to a network transport. It's used to open and
// listen for connections that are handled by the Distributer to allocate and
// synchronize Runners across multiple nodes in a cluster.
//
// NOTE It MUST be the case that all connections that were opened with Dial()
// will arrive to the other side in the Listen() function
type Transport interface {
    // Transport is a listener, able to provide the Distributer with incoming
    // connections. NOTE that the Address() will be matched against the
    // addresses provided to the Distributer to determine which node is the
    // one currently running
    net.Listener

    // Dial should open a connection to a remote address. The address will be
    // the string address provided to the Distributer.
    Dial(addr string) (net.Conn, error)
}

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {

    // Distribute a Runner to multiple node addresses
    Distribute(runner Runner, addrs ...string) error

    // Start listening for incoming Runners to run
    Start() error // blocks.

    // Stop listening for incoming Runners to run, and close all open
    // connections.
    Close() error
}

// NewDistributer creates a Distributer that can be used to distribute work of
// Runners across multiple nodes in a cluster. Distributer must be started on
// all node peers in order for them to receive work.
func NewDistributer(transport Transport) Distributer {
    return &distributer{transport, make(map[string]net.Conn)}
}

type distributer struct {
    transport Transport
    connsMap map[string]net.Conn
}

func (d *distributer) Start() error {
    for {
        conn, err := d.transport.Accept()
        if err != nil {
            return err
        }

        go d.Serve(conn)
    }
}

func (d *distributer) Close() error {
    return d.transport.Close()
}

func (d *distributer) Distribute(runner Runner, addrs ...string) error {
    for _, addr := range d.addrs {
        conn, err := d.transport.Dial(addr)
        if err != nil {
            return err
        }

        defer conn.Close()
        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.transport.Address(), runner, addrs, nil})
        if err != nil {
            return err
        }
    }

    return nil
}

// Connect to a node address for the given uid. Used by the individual exchange
// runners to synchronize a specific logical point in the code. We need to
// ensure that both sides of the connection, when used with the same Uid,
// resolve to the same connection
func (d *distributer) Connect(addr string, uid string) (net.Conn, error) {
    from := d.transport.Address()

    if from < addr {
        // dial
        conn, err := d.transport.Dial(addr)
        if err != nil {
            return nil, err
        }

        err = enc.Encode(&req{d.transport.Address(), nil, nil, uid})
        if err != nil {
            return nil, err
        }

        // wait for an ack on the other side

    } else {
        // listen
        conn := <- d.connCh(addr, uid)

        // send the ack
        err = enc.Encode(&req{d.transport.Address(), nil, nil, uid})
        if err != nil {
            return nil, err
        }
    }

    return conn, err
}

func (d *distributer) Serve(conn net.Conn) error {
    r := &req{}
    dec := gob.NewDecoder(conn)
    err := dec.Decode(r)
    if err != nil {
        return err
    }

    if r.Runner != nil {
        ctx := context.Background()
        ctx = context.WithValue(ctx, "ep.AllNodes", r.RunnerNodes)
        ctx = context.WithValue(ctx, "ep.MasterNodes", r.From)
        ctx = context.WithValue(ctx, "ep.ThisNode", d.transport.Address())
        return header.Runner.Run(ctx, inp, out)
    } else {

        // wait for someone to claim it.
        d.connCh(r.From, r.Uid) <- conn
    }
    return nil
}

func (d *distributer) connCh(addr, uid string) (chan net.Conn) {
    k := addr + ":" + uid
    d.l.Lock()
    defer d.l.Unlock()
    if d.connsMap[k] == nil {
        d.connsMap[k] = make(chan net.Conn)
    }
    return d.connsMap[k]
}

type req struct {
    From string // Address of the transport issuing the request
    Runner Runner
    RunnerNodes []string
    Uid string
}
