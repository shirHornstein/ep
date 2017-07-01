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

var _ = registerGob(&distRunner{})

// Distributer is an object that can distribute Runners to run in parallel on
// multiple nodes.
type Distributer interface {

    // Distribute a Runner to multiple node addresses
    Distribute(runner Runner, addrs ...string) Runner

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
    connsMap := make(map[string]chan net.Conn)
    closeCh := make(chan error, 1)
    d := &distributer{listener, addr, connsMap, &sync.Mutex{}, closeCh}
    go d.start()
    return d
}

type distributer struct {
    listener net.Listener
    addr string
    connsMap map[string]chan net.Conn
    l sync.Locker
    closeCh chan error
}

func (d *distributer) start() error {
    d.l.Lock()
    closeCh := d.closeCh
    d.l.Unlock()

    if closeCh == nil {
        return nil // closed.
    }

    defer close(closeCh)
    for {
        conn, err := d.listener.Accept()
        if err != nil {
            return err
        }

        go d.Serve(conn)
    }
}

func (d *distributer) Close() error {
    d.l.Lock()
    closeCh := d.closeCh
    d.closeCh = nil // prevent all future function calls
    d.l.Unlock()

    if closeCh == nil { // not running.
        return nil
    }

    err := d.listener.Close()
    if err != nil {
        return err
    }

    // wait for start() above to exit. otherwise, tests or code that attempts to
    // re-bind to the same address will infrequently fail with "bind: address
    // already in use" because while the listener is closed, there's still one
    // pending Accept()
    // TODO: consider waiting for all served connections/runners?
    <- closeCh
    return nil
}

func (d *distributer) dial(addr string) (net.Conn, error) {
    if d.closeCh == nil {
        return nil, io.ErrClosedPipe
    }

    dialer, ok := d.listener.(dialer)
    if ok {
        return dialer.Dial("tcp", addr)
    }

    return net.Dial("tcp", addr)
}

func (d *distributer) Distribute(runner Runner, addrs ...string) Runner {
    return &distRunner{runner, addrs, d.addr, d}
}

// Connect to a node address for the given uid. Used by the individual exchange
// runners to synchronize a specific logical point in the code. We need to
// ensure that both sides of the connection, when used with the same UID,
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
    typee, err := readStr(conn)
    if err != nil {
        return err
    }

    if typee == "D" { // data connection
        key, err := readStr(conn)
        if err != nil {
            return err
        }

        // wait for someone to claim it.
        d.connCh(key) <- conn
    } else if (typee == "X") { // execute runner connection
        defer conn.Close()

        r := &distRunner{d: d}
        dec := gob.NewDecoder(conn)
        err := dec.Decode(r)
        if err != nil {
            fmt.Println("ep: distributer error", err)
            return err
        }

        // drain the output
        // generally - if we're always using Gather, the output will be empty
        // perhaps we want to log/return an error when some of the data is
        // discarded here?
        out := make(chan Dataset)
        go func() { for _ = range out {} }()

        inp := make(chan Dataset, 1)
        close(inp)

        err = r.Run(context.Background(), inp, out)
        enc := gob.NewEncoder(conn)
        if err != nil {
             err = &errMsg{err.Error()}
        }

        err = enc.Encode(&dataReq{err})
        if err != nil {
            fmt.Println("ep: runner error", err)
            return err
        }
    } else {
        defer conn.Close()

        err := fmt.Errorf("unrecognized connection type: %s", typee)
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

// distRunner wraps around a runner, and upon the initial call to Run, it
// distributes the runner to all nodes and runs them in parallel.
type distRunner struct {
    Runner
    Addrs []string // participating node addresses
    MasterAddr string // the master node that created the distRunner
    d *distributer
}

func (r *distRunner) Run(ctx context.Context, inp, out chan Dataset) error {
    decs := []*gob.Decoder{}
    isMain := r.d.addr == r.MasterAddr
    for i := 0 ; i < len(r.Addrs) && isMain ; i++ {
        addr := r.Addrs[i]
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
        err = enc.Encode(r)
        if err != nil {
            return err
        }

        decs = append(decs, gob.NewDecoder(conn))
    }

    ctx = context.WithValue(ctx, "ep.AllNodes", r.Addrs)
    ctx = context.WithValue(ctx, "ep.MasterNode", r.MasterAddr)
    ctx = context.WithValue(ctx, "ep.ThisNode", r.d.addr)
    ctx = context.WithValue(ctx, "ep.Distributer", r.d)

    err := r.Runner.Run(ctx, inp, out)
    if err != nil {
        return err
    }

    // collect error responses
    // NB. currently the errors will arrive here in two ways: the exchange
    // Runners communicate errors between nodes such that this master node will
    // always receive the errors from the individual nodes. The final error is
    // also transmitted by the Distributer at the end of the remote Run. This
    // might be redundant - but in any case we need the top-level one here to
    // make sure we wait for all runners to complete, thus not leaving any open
    // resources/goroutines.
    for _, dec := range decs {
        req := &dataReq{}
        err = dec.Decode(req)
        data := req.Payload
        if err == nil {
            err, _ = data.(error)
        }

        if err != nil {
            return err
        }
    }

    return nil
}


// write a null-terminated string to a writer
func writeStr(w io.Writer, s string) error {
    _, err := w.Write(append([]byte(s), 0))
    return err
}

// read a null-terminated string from a reader
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
