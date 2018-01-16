package ep

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"net"
)

var _ = registerGob(&exchange{}, &req{}, &errMsg{})

const (
	sendGather    = 1
	sendScatter   = 2
	sendBroadcast = 3
	sendPartition = 4
)

// Scatter returns an exchange Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a round-
// robin to the nodes.
func Scatter() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), SendTo: sendScatter}
}

// Gather returns an exchange Runner that gathers all of its input into a
// single node. In all other nodes it will produce no output, but on the main
// node it will be passthrough from all of the other nodes
func Gather() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), SendTo: sendGather}
}

// Broadcast returns an exchange Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all of the inputs from
// all nodes (order not guaranteed)
func Broadcast() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), SendTo: sendBroadcast}
}

// exchange is a Runner that exchanges data between peer nodes
type exchange struct {
	UID    string
	SendTo int

	encs     []encoder   // encoders to all destination connections
	decs     []decoder   // decoders from all source connections
	conns    []io.Closer // all open connections (used for closing)
	encsNext int         // Encoders Round Robin next index
	decsNext int         // Decoders Round Robin next index
}

func (ex *exchange) Returns() []Type { return []Type{Wildcard} }
func (ex *exchange) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	defer func() {
		closeErr := ex.Close()
		// prefer real existing error over close error
		if err == nil {
			err = closeErr
		}
	}()

	err = ex.Init(ctx)
	if err != nil {
		return err
	}

	// receive remote data from peers in a go-routine. Write the final error (or
	// nil) to the channel when done.
	errs := make(chan error)
	go func() {
		defer close(errs)
		for {
			data, err := ex.Receive()
			if err == io.EOF {
				break
			} else if err != nil {
				errs <- err
				return
			}
			out <- data
		}
		errs <- nil
	}()

	// send the local data to the peers, until completion or error. Also listen
	// for the completion of the received go-routine above. When both sending
	// and receiving is complete, exit. Upon error, exit early.
	rcvDone := false
	sndDone := false
	for err == nil && (!rcvDone || !sndDone) {
		select {
		case data, ok := <-inp:
			if !ok {
				// the input is exhausted. Notify peers that we're done sending
				// data (they will use it to stop listening to data from us).
				eofMsg := &errMsg{io.EOF.Error()}
				notifiedAllErr := ex.EncodeAll(eofMsg)
				if notifiedAllErr != nil {
					//TODO avia
				}
				sndDone = true

				// inp is closed. If we keep iterating, it will infinitely
				// resolve to (nil, true). Nill-ify it to block it on the next
				// iteration.
				inp = nil
				continue
			}

			err = ex.Send(data)
		case err = <-errs:
			rcvDone = true // errors (or nil) from the receive go-routine
		case <-ctx.Done():
			err = ctx.Err() // context timeout or cancel
		}
	}
	return err
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
	return ex.DecodeNext()
}

// Close all open connections
func (ex *exchange) Close() (err error) {
	for _, conn := range ex.conns {
		err1 := conn.Close()
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// Encode an object to all destination connections
func (ex *exchange) EncodeAll(e interface{}) (err error) {
	req := &req{e}
	for _, enc := range ex.encs {
		err1 := enc.Encode(req)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// Encode an object to the next destination connection in a round robin
func (ex *exchange) EncodeNext(e interface{}) error {
	if len(ex.encs) == 0 {
		return io.ErrClosedPipe
	}
	if e, isErr := e.(error); isErr {
		panic("got an error to encode: " + e.Error())
	}

	req := &req{e}
	ex.encsNext = (ex.encsNext + 1) % len(ex.encs)
	return ex.encs[ex.encsNext].Encode(req)
}

// Encode an object to a destination connection selected by partitioning
func (ex *exchange) EncodePartition(e interface{}) error {
	return nil
}

// Decode an object from the next source connection in a round robin
func (ex *exchange) DecodeNext() (Dataset, error) {
	if len(ex.decs) == 0 {
		return nil, io.EOF
	}

	i := (ex.decsNext + 1) % len(ex.decs)

	req := &req{}
	err := ex.decs[i].Decode(req)
	if err != nil {
		if err == io.EOF {
			// remove the current decoder and try again
			ex.decs = append(ex.decs[:i], ex.decs[i+1:]...)
			return ex.DecodeNext()
		}
		return nil, err
	}

	ex.decsNext = i

	data, ok := req.Payload.(Dataset)
	if !ok {
		panic("please exchange only datasets! :(")
	}
	return data, nil
}

// initialize the connections, encoders & decoders
func (ex *exchange) Init(ctx context.Context) (err error) {
	dist, _ := ctx.Value(distributerKey).(interface {
		Connect(addr, uid string) (net.Conn, error)
	})

	if dist == nil {
		return fmt.Errorf("exhcnage started without a distributer")
	}

	allNodes := ctx.Value(allNodesKey).([]string)
	thisNode := ctx.Value(thisNodeKey).(string)
	masterNode := ctx.Value(masterNodeKey).(string)

	targetNodes := allNodes
	if ex.SendTo == sendGather {
		targetNodes = []string{masterNode}
	}

	// open a connection to all target nodes
	connsMap := map[string]net.Conn{}
	var shortCircuit *shortCircuit
	defer func() {
		if err != nil {
			// in case of error in one connection. close all other connections
			ex.Close()
		}
	}()
	var conn net.Conn
	for _, n := range targetNodes {
		if n == thisNode {
			shortCircuit = newShortCircuit()
			ex.conns = append(ex.conns, shortCircuit)
			ex.encs = append(ex.encs, shortCircuit)
			continue
		}

		msg := "THIS " + thisNode + " OTHER " + n

		conn, err = dist.Connect(n, ex.UID)
		if err != nil {
			return err
		}

		connsMap[n] = conn
		ex.conns = append(ex.conns, conn)
		ex.encs = append(ex.encs, dbgEncoder{gob.NewEncoder(conn), msg})
	}

	// if we're also a destination, listen to all nodes
	for i := 0; shortCircuit != nil && i < len(allNodes); i++ {
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

		conn, err = dist.Connect(n, ex.UID)
		if err != nil {
			return err
		}

		ex.conns = append(ex.conns, conn)
		ex.decs = append(ex.decs, dbgDecoder{gob.NewDecoder(conn), msg})
	}

	return nil
}

// interfaces for gob.Encoder/Decoder. Used to also implement the short-circuit.
type encoder interface {
	Encode(interface{}) error
}
type decoder interface {
	Decode(interface{}) error
}

type dbgEncoder struct {
	encoder
	msg string
}

func (enc dbgEncoder) Encode(e interface{}) error {
	// fmt.Println("ENCODE", enc.msg, e)
	err := enc.encoder.Encode(e)
	// fmt.Println("ENCODE DONE", enc.msg, e, err)
	return err
}

type dbgDecoder struct {
	decoder
	msg string
}

func (dec dbgDecoder) Decode(e interface{}) error {
	// fmt.Println("DECODE", dec.msg)
	err := dec.decoder.Decode(e)
	if err == nil && isEOFError(e) {
		return io.EOF
	}
	// fmt.Println("DECODE DONE", dec.msg, e, err)
	return err
}

// shortCircuit implements io.Closer, encoder and decoder and provides the
// means to short-circuit internal communications within the same node. This is
// in order to not complicate the generic nature of the exchange code
type shortCircuit struct {
	C      chan interface{}
	closed bool
	all    []interface{}
}

func (sc *shortCircuit) Close() error {
	if sc.closed {
		return nil
	}

	sc.closed = true
	close(sc.C)
	// fmt.Println("SC: Closed")
	return nil
}

func (sc *shortCircuit) Encode(e interface{}) error {
	if sc.closed {
		return io.ErrClosedPipe
	}

	sc.C <- e
	// fmt.Println("SC: Encoded", e)
	return nil
}

func (sc *shortCircuit) Decode(e interface{}) error {
	v, ok := <-sc.C
	if !ok || isEOFError(v) {
		return io.EOF
	}
	*e.(*req) = *v.(*req)
	return nil
}

func newShortCircuit() *shortCircuit {
	return &shortCircuit{C: make(chan interface{}, 1000)}
}

type req struct{ Payload interface{} }
type errMsg struct{ Msg string }

func (err *errMsg) Error() string { return err.Msg }

func isEOFError(data interface{}) bool {
	err, isErr := data.(*req).Payload.(error)
	return isErr && err.Error() == io.EOF.Error()
}
