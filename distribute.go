package ep

import (
    "io"
    "net"
    "fmt"
    "sync"
    "time"
    "context"
    "encoding/gob"
)

var _ = registerGob(req{}, &distRunner{})

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {

    // Distribute a Runner to multiple node addresses. `this` is the address of
    // the current node issuing this distribution.
    Distribute(runner Runner, addrs ...string) (Runner, error)

    // Start listening for incoming Runners to run
    Start() error // blocks.

    // Stop listening for incoming Runners to run, and close all open
    // connections.
    Close() error
}

type dialer interface {
    Dial(network, addr string) (net.Conn, error)
}

// NewDistributer creates a Distributer that can be used to distribute work of
// Runners across multiple nodes in a cluster. Distributer must be started on
// all node peers in order for them to receive work. You can also implement the
// dialer interface (implemented by net.Dialer) in order to provide your own
// connections:
//
//      type dialer interface {
//          Dial(network, addr string) (net.Conn, error)
//      }
//
func NewDistributer(addr string, listener net.Listener) Distributer {
    return &distributer{listener, addr, make(map[string]chan net.Conn), &sync.Mutex{}, nil}
}

type distributer struct {
    listener net.Listener
    addr string
    connsMap map[string]chan net.Conn
    l sync.Locker
    closeCh chan error
}

func (d *distributer) Start() error {
    d.l.Lock()
    d.closeCh = make(chan error, 1)
    defer close(d.closeCh)
    d.l.Unlock()

    for {
        conn, err := d.listener.Accept()
        if err != nil {
            return err
        }

        go d.Serve(conn)
    }
}

func (d *distributer) Close() error {
    err := d.listener.Close()
    if err != nil {
        return err
    }

    // wait for Start() above to exit. otherwise, tests or code that attempts to
    // re-bind to the same address will infrequently fail with "bind: address
    // already in use" because while the listener is closed, there's still one
    // pending Accept()
    // TODO: consider waiting for all served connections/runners?
    d.l.Lock()
    defer d.l.Unlock()
    if d.closeCh != nil {
        <- d.closeCh
    }
    return nil
}

func (d *distributer) dial(addr string) (net.Conn, error) {
    dialer, ok := d.listener.(dialer)
    if ok {
        return dialer.Dial("tcp", addr)
    }

    return net.Dial("tcp", addr)
}

func (d *distributer) Distribute(runner Runner, addrs ...string) (Runner, error) {
    return &distRunner{runner, addrs, d.addr, d, true}, nil


    for _, addr := range addrs {
        if addr == d.addr {
            continue
        }

        conn, err := d.dial(addr)
        if err != nil {
            return nil, err
        }

        err = writeStr(conn, "R") // runner connection
        if err != nil {
            return nil, err
        }

        defer conn.Close()
        if err != nil {
            return nil, err
        }

        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{d.addr, runner, addrs, ""})
        if err != nil {
            return nil, err
        }
    }

    runner = WithValue(runner, "ep.AllNodes", addrs)
    runner = WithValue(runner, "ep.MasterNode", d.addr)
    runner = WithValue(runner, "ep.ThisNode", d.addr)
    runner = WithValue(runner, "ep.Distributer", d)
    return runner, nil
}

// Connect to a node address for the given uid. Used by the individual exchange
// runners to synchronize a specific logical point in the code. We need to
// ensure that both sides of the connection, when used with the same Uid,
// resolve to the same connection
func (d *distributer) Connect(addr string, uid string) (conn net.Conn, err error) {
    from := d.addr
    if from < addr {
        // dial
        conn, err = d.dial(addr)
        if err != nil {
            return
        }

        err = writeStr(conn, "D") // Data connection
        if err != nil {
            return
        }

        err = writeStr(conn, d.addr + ":" + uid)
        if err != nil {
            return
        }
    } else {
        // listen, timeout after 1 second
        timer := time.NewTimer(time.Second)
        defer timer.Stop()

        select {
        case conn = <- d.connCh(addr + ":" + uid):
            // let it through
        case <- timer.C:
            err = fmt.Errorf("ep: connect timeout; no incoming conn")
        }
    }

    return conn, err
}

func (d *distributer) Serve(conn net.Conn) error {
    type_, err := readStr(conn)
    if err != nil {
        return err
    }

    if type_ == "D" { // data connection
        key, err := readStr(conn)
        if err != nil {
            return err
        }

        // wait for someone to claim it.
        d.connCh(key) <- conn
    } else if (type_ == "X") { // execute runner connection
        r := &req{}
        dec := gob.NewDecoder(conn)
        err := dec.Decode(r)
        if err != nil {
            fmt.Println("ep: distributer error", err)
            return err
        }

        runner := r.Runner.(*distRunner)
        runner.d = d

        out := make(chan Dataset)
        inp := make(chan Dataset, 1)
        close(inp)

        err = runner.Run(context.Background(), inp, out)
        if err != nil {
            fmt.Println("ep: runner error", err)
            return err
        }

    } else {
        err := fmt.Errorf("unrecognized connection type: %s", type_)
        fmt.Println("ep: " + err.Error())
        return err
    }

    return nil
}

func (d *distributer) connCh(k string) (chan net.Conn) {
    // k := addr + ":" + uid
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

type distRunner struct { Runner; Nodes []string; Master string; d *distributer; isMain bool }

func (r *distRunner) Run(ctx context.Context, inp, out chan Dataset) error {
    for i := 0 ; i < len(r.Nodes) && r.isMain ; i++ {
        addr := r.Nodes[i]
        if addr == r.d.addr {
            continue
        }

        conn, err := r.d.dial(addr)
        if err != nil {
            return err
        }

        err = writeStr(conn, "X") // runner connection
        if err != nil {
            return err
        }

        defer conn.Close()
        if err != nil {
            return err
        }

        enc := gob.NewEncoder(conn)
        err = enc.Encode(&req{r.d.addr, r, nil, ""})
        if err != nil {
            return err
        }
    }

    ctx = context.WithValue(ctx, "ep.AllNodes", r.Nodes)
    ctx = context.WithValue(ctx, "ep.MasterNode", r.Master)
    ctx = context.WithValue(ctx, "ep.ThisNode", r.d.addr)
    ctx = context.WithValue(ctx, "ep.Distributer", r.d)

    return r.Runner.Run(ctx, inp, out)
}


// write a null-terminated string to a writer
func writeStr(w io.Writer, s string) error {
    _, err := w.Write(append([]byte(s), 0))
    return err
}

func readStr(r io.Reader) (s string, err error) {
    b := []byte{0}
    for {
        _, err = r.Read(b)
        if err != nil {
            return
        } else if b[0] == 0 {
            return
        }

        s += string(b[0])
    }
}
