package ep

import (
    "io"
    "net"
    "fmt"
    "time"
    "sync"
    "context"
    "encoding/gob"
    "github.com/satori/go.uuid"
)

var _ = registerGob(&exchange{}, &errorData{})

const (
    sendGather = 1
    sendScatter = 2
    sendBroadcast = 3
    sendPartition = 4
)

// Scatter returns an exchange Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a round-
// robin to the nodes.
func Scatter() Runner {
    return &exchange{Uid: uuid.NewV1().String(), SendTo: sendScatter}
}

// Gather returns an exchange Runner that gathers all of its input into a
// single node. In all other nodes it will produce no output, but on the main
// node it will be passthrough from all of the other nodes
func Gather() Runner {
    return nil
}

// Broadcast returns an exchange Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all of the inputs from
// all nodes (order not guaranteed)
func Broadcast() Runner {
    return nil
}

// exchange is a Runner that exchanges data between peer nodes
type exchange struct {
    Uid string
    SendTo int

    encs []encoder // encoders to all destination connections
    decs []decoder // decoders from all source connections
    conns []io.Closer // all open connections (used for closing)
    encsNext int // Encoders Round Robin next index
    decsNext int // Decoders Round Robin next index
}

func (ex *exchange) Returns() []Type { return []Type{Wildcard} }
func (ex *exchange) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    var wg sync.WaitGroup
    defer wg.Wait() // don't leak the go-routine

    thisNode := ctx.Value("ep.ThisNode").(string)
    defer func() { ex.Close(err) }()

    err = ex.Init(ctx)
    if err != nil {
        return
    }

    wg.Add(1)
    go func() {
        defer wg.Done()
        defer ex.EncodeAll(nil)
        for data := range inp {
            err = ex.Send(data)
            if err != nil {
                ex.Close(err)
                return
            }
        }
    }()

    for {
        data, err := ex.Receive()
        if err == io.EOF {
            return nil
        } else if err != nil {
            ex.Close(err)
            return err
        }

        out <- data
    }

    return

    go func() {
        for data := range inp {
            err = ex.Send(data)
            if err != nil {
                fmt.Println("Send Err", thisNode, err)
                ex.Close(err)
                return
            }
        }

        err = ex.EncodeAll(nil) // TODO: replace with a real marker Dataset
        if err != nil {
            fmt.Println("Send Nil Err", thisNode, err)
            ex.Close(err)
        }
    }()

    // listen to all nodes for incoming data
    // var data Dataset
    for {
        data, err1 := ex.Receive()
        if err1 != nil {
            if err1 == io.EOF {
                err = nil
            } else if err == nil {
                fmt.Println("Receive Err", thisNode, err1, err)
                err = err1
            }

            return
        }

        out <- data
    }
}

// Send a dataset to destination nodes
func (ex *exchange) Send(data Dataset) error {
    switch ex.SendTo {
    case sendScatter:
        return ex.EncodeNext(data)
    case sendPartition:
        return ex.EncodePartition(data)
    default:
        return ex.EncodeAll(data)
    }
}

func (ex *exchange) Receive() (Dataset, error) {
    data := NewDataset()
    err := ex.DecodeNext(&data)
    if err != nil {
        return nil, err
    }

    err, ok := data.(*errorData)
    if ok {
        return nil, err
    }
    return data, nil
}

// Close all open connections. If an error object is supplied, it's first
// encoded to all connections before closing.
func (ex *exchange) Close(err error) error {
    var errOut error
    if err != nil {
        var errData Dataset = &errorData{Err: err.Error()}
        errOut = ex.EncodeAll(errData)

        // the error below is triggered very infrequently when we hang up too
        // fast. 1ms timeout in case of error is a good tradeoff compared to
        // the complexity of locks or a full hand-shake here.
        time.Sleep(1 * time.Millisecond) // use of closed network connection
    }

    for _, conn := range ex.conns {
        err1 := conn.Close()
        if err1 != nil {
            errOut = err1
        }
    }

    return errOut
}

// Encode an object to all destination connections
func (ex *exchange) EncodeAll(e Dataset) (err error) {
    for _, enc := range ex.encs {
        err1 := enc.Encode(&e)
        if err1 != nil {
            err = err1
        }
    }

    return err
}

// Encode an object to the next destination connection in a round robin
func (ex *exchange) EncodeNext(e Dataset) error {
    if len(ex.encs) == 0 {
        return io.ErrClosedPipe
    }

    ex.encsNext = (ex.encsNext + 1) % len(ex.encs)
    return ex.encs[ex.encsNext].Encode(&e)
}

// Encode an object to a destination connection selected by partitioning
func (ex *exchange) EncodePartition(e Dataset) error {
    return nil
}

// Decode an object from the next source connection in a round robin
func (ex *exchange) DecodeNext(e *Dataset) error {
    if len(ex.decs) == 0 {
        return io.EOF
    }

    i := (ex.decsNext + 1) % len(ex.decs)
    err := ex.decs[i].Decode(e)
    if err != nil && err != io.EOF {
        return err
    } else if err == io.EOF || *e == nil {
        // remove the current decoder and try again
        ex.decs = append(ex.decs[:i], ex.decs[i + 1:]...)
        return ex.DecodeNext(e)
    }

    ex.decsNext = i
    return nil
}

// initialize the connections, encoders & decoders
func (ex *exchange) Init(ctx context.Context) error {
    var err error

    allNodes := ctx.Value("ep.AllNodes").([]string)
    thisNode := ctx.Value("ep.ThisNode").(string)
    masterNode := ctx.Value("ep.MasterNode").(string)
    dist := ctx.Value("ep.Distributer").(interface {
        Connect(addr, uid string) (net.Conn, error)
    })

    targetNodes := allNodes
    if ex.SendTo == sendGather {
        targetNodes = []string{masterNode}
    }

    // open a connection to all target nodes
    var conn net.Conn
    connsMap := map[string]net.Conn{}
    var shortCircuit *shortCircuit
    for _, n := range targetNodes {
        if n == thisNode {
            shortCircuit = newShortCircuit()
            ex.conns = append(ex.conns, shortCircuit)
            ex.encs = append(ex.encs, shortCircuit)
            continue
        }

        msg := "THIS " + thisNode + " OTHER " + n

        conn, err = dist.Connect(n, ex.Uid)
        if err != nil {
            return err
        }

        connsMap[n] = conn
        ex.conns = append(ex.conns, conn)
        ex.encs = append(ex.encs, dbgEncoder{gob.NewEncoder(conn), msg})
    }

    // if we're also a destination, listen to all nodes
    for i := 0; shortCircuit != nil && i < len(allNodes); i += 1 {
        n := allNodes[i]

        if n == thisNode {
            ex.decs = append(ex.decs, shortCircuit)
            continue
        }

        msg := "THIS " + thisNode + " OTHER " + n

        // if we already established a connection to this node from the targets,
        // re-use it. We don't need 2 uni-directional connections.
        if connsMap[n] != nil {
            ex.decs = append(ex.decs, dbgDecoder{gob.NewDecoder(connsMap[n]), msg})
            continue
        }

        conn, err = dist.Connect(n, ex.Uid)
        if err != nil {
            return err
        }

        ex.conns = append(ex.conns, conn)
        ex.decs = append(ex.decs, dbgDecoder{gob.NewDecoder(conn), msg})
    }

    return nil
}

// interfqace for gob.Encoder/Decoder. Used to also implement the short-circuit.
type encoder interface { Encode(interface{}) error }
type decoder interface { Decode(interface{}) error }

type dbgEncoder struct { encoder; msg string }
func (enc dbgEncoder) Encode(e interface{}) error {
    // fmt.Println("ENCODE", enc.msg, e)
    err := enc.encoder.Encode(e)
    // fmt.Println("ENCODE DONE", enc.msg, e, err)
    return err
}

type dbgDecoder struct { decoder; msg string }
func (dec dbgDecoder) Decode(e interface{}) error {
    // fmt.Println("DECODE", dec.msg)
    err := dec.decoder.Decode(e)
    // fmt.Println("DECODE DONE", dec.msg, e, err)
    return err
}

// shortCircuit implements io.Closer, encoder and dedocder and provides the
// means to short-circuit internal communications within the same node. This is
// in order to not complicate the generic nature of the exchange code
type shortCircuit struct {
    C chan interface{};
    Closed bool
}

func (sc *shortCircuit) Close() error {
    if sc.Closed {
        return nil
    }

    sc.Closed = true
    close(sc.C);
    // fmt.Println("SC: Closed")
    return nil
}

func (sc *shortCircuit) Encode(e interface{}) error {
    if sc.Closed {
        return io.ErrClosedPipe
    }

    // fmt.Println("SC: Encode", e)
    sc.C <- e;
    // fmt.Println("SC: Encoded", e)
    return nil
}

func (sc *shortCircuit) Decode(e interface{}) error {
    if sc.Closed {
        return io.ErrClosedPipe
    }

    data := e.(*Dataset)

    // fmt.Println("SC: Decode")
    v, ok := <- sc.C
    // fmt.Println("SC: Decoded", v, ok)
    if !ok {
        return io.EOF
    }

    if v == nil {
        *data = nil
    } else {
        // fmt.Println("SC: Casting", v)
        *data = *v.(*Dataset)
        // fmt.Println("SC: Casted")
    }

    return nil
}

func newShortCircuit() *shortCircuit {
    return &shortCircuit{C: make(chan interface{}, 1000)}
}


type errorData struct { Dataset; Err string }
func (*errorData) String() string { return "errorData" }
func (data *errorData) Error() string { return data.Err }
