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

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {

    // Distribute a Runner to multiple node addresses. `this` is the address of
    // the current node issuing this distribution.
    Distribute(runner Runner, this string, addrs ...string) (Runner, error)

    // Start listening for incoming Runners to run
    Start() error // blocks.

    // Stop listening for incoming Runners to run, and close all open
    // connections.
    Close() error
}

type dialer interface {
    Dial(addr string) (net.Conn, error)
}

// NewDistributer creates a Distributer that can be used to distribute work of
// Runners across multiple nodes in a cluster. Distributer must be started on
// all node peers in order for them to receive work. You can also implement the
// dialer interface (below) in order to provide your own connections:
//
//      type dialer interface {
//          Dial(addr string) (net.Conn, error)
//      }
//
func NewDistributer(listener net.Listener) Distributer {
    return &distributer{listener, make(map[string]chan net.Conn), &sync.Mutex{}}
}

type distributer struct {
    listener net.Listener
    connsMap map[string]chan net.Conn
    l sync.Locker
}

func (d *distributer) Start() error {
    for {
        conn, err := d.listener.Accept()
        if err != nil {
            return err
        }

        go d.Serve(conn)
    }
}

func (d *distributer) Close() error {
    return d.listener.Close()
}

func (d *distributer) dial(addr string) (net.Conn, error) {
    dialer, ok := d.listener.(dialer)
    if ok {
        return dialer.Dial(addr)
    }

    return net.Dial("tcp", addr)
}

func (d *distributer) Distribute(runner Runner, this string, addrs ...string) (Runner, error) {
    for _, addr := range addrs {
        conn, err := d.dial(addr)
        if err != nil {
            return nil, err
        }

        defer conn.Close()
        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{this, runner, addrs, ""})
        if err != nil {
            return nil, err
        }
    }

    runner = WithValue(runner, "ep.AllNodes", addrs)
    runner = WithValue(runner, "ep.MasterNode", this)
    runner = WithValue(runner, "ep.ThisNode", this)
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

    from := d.listener.Addr().String()
    if from < addr {
        // dial
        conn, err = d.dial(addr)
        if err != nil {
            return nil, err
        }

        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.listener.Addr().String(), nil, nil, uid})
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
        err = enc.Encode(&req{d.listener.Addr().String(), nil, nil, uid})
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
        fmt.Println("ep: distributer error", err)
        return err
    }

    if r.Runner != nil {
        ctx := context.Background()

        runner := r.Runner
        runner = WithValue(runner, "ep.AllNodes", r.RunnerNodes)
        runner = WithValue(runner, "ep.MasterNode", r.From)
        runner = WithValue(runner, "ep.ThisNode", d.listener.Addr().String())
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
