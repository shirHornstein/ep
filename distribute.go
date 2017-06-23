package ep

import (
    "net"
    "fmt"
    "sync"
    "time"
    "context"
    "encoding/gob"
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
    Distribute(runner Runner, addrs ...string) (Runner, error)

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
    return &distributer{transport, make(map[string]chan net.Conn), &sync.Mutex{}}
}

type distributer struct {
    transport Transport
    connsMap map[string]chan net.Conn
    l sync.Locker
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

func (d *distributer) Distribute(runner Runner, addrs ...string) (Runner, error) {
    for _, addr := range addrs {
        conn, err := d.transport.Dial(addr)
        if err != nil {
            return nil, err
        }

        defer conn.Close()
        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.transport.Addr().String(), runner, addrs, ""})
        if err != nil {
            return nil, err
        }
    }

    runner = WithValue(runner, "ep.AllNodes", addrs)
    runner = WithValue(runner, "ep.MasterNode", d.transport.Addr().String())
    runner = WithValue(runner, "ep.ThisNode", d.transport.Addr().String())
    runner = WithValue(runner, "ep.Distributer", d)
    return runner, nil
}

// Connect to a node address for the given uid. Used by the individual exchange
// runners to synchronize a specific logical point in the code. We need to
// ensure that both sides of the connection, when used with the same Uid,
// resolve to the same connection
func (d *distributer) Connect(addr string, uid string) (net.Conn, error) {
    var err error
    var conn net.Conn

    from := d.transport.Addr().String()
    if from < addr {
        // dial
        conn, err = d.transport.Dial(addr)
        if err != nil {
            return nil, err
        }

        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.transport.Addr().String(), nil, nil, uid})
        if err != nil {
            return nil, err
        }

        // wait for an ack on the other side

    } else {
        // listen, timeout after 1 second
        timer := time.NewTimer(time.Second)
        select {
        case conn = <- d.connCh(addr, uid):
            // let it through
        case <- timer.C:
            err = fmt.Errorf("ep: connect timeout; no incoming conn")
        }

        timer.Stop()
        if err != nil {
            return nil, err
        }

        // send the ack
        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.transport.Addr().String(), nil, nil, uid})
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

        runner := r.Runner
        runner = WithValue(runner, "ep.AllNodes", r.RunnerNodes)
        runner = WithValue(runner, "ep.MasterNode", r.From)
        runner = WithValue(runner, "ep.ThisNode", d.transport.Addr().String())
        runner = WithValue(runner, "ep.Distributer", d)

        out := make(chan Dataset)
        inp := make(chan Dataset, 1)
        close(inp)

        return runner.Run(ctx, inp, out)
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

// WithValue creates a new Runner that adds the key and value to the context
// before calling its internal input Runner
func WithValue(r Runner, k, v interface{}) Runner {
    return &ctxRunner{r, k, v}
}

type ctxRunner struct { Runner; K interface{}; V interface{} }
func (r *ctxRunner) Run(ctx context.Context, inp, out chan Dataset) error {
    return r.Runner.Run(context.WithValue(ctx, r.K, r.V), inp, out)
}
