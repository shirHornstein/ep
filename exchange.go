package ep

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/panoplyio/go-consistent"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"io"
	"net"
	"sync"
)

var _ = registerGob(&exchange{}, &req{}, &errMsg{})

type exchangeType int

const (
	gather exchangeType = iota
	sortGather
	scatter
	broadcast
	partition
)

type req struct{ Payload interface{} }

type errMsg struct{ Msg string }

func (err *errMsg) Error() string { return err.Msg }

var errOnPeer = errors.New("E")
var errorMsg = &errMsg{errOnPeer.Error()}
var eofMsg = &errMsg{io.EOF.Error()}

// Gather returns an exchange Runner that gathers all of its input into a
// single node. On the main node it will passThrough data from all other
// nodes, and will produce no output on peers
func Gather() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: gather}
}

// Broadcast returns an exchange Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all inputs from
// all nodes. Order not guaranteed
func Broadcast() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: broadcast}
}

// exchange is a Runner that exchanges data between peer nodes
type exchange struct {
	UID  string
	Type exchangeType

	inited   bool        // was this runner initialized
	encs     []encoder   // encoders to all destination connections
	decs     []decoder   // decoders from all source connections
	encsErr  []encoder   // encoders to all peers to propagate errors
	decsErr  []decoder   // decoders from all peers to propagate errors
	conns    []io.Closer // all open connections (used for closing)
	encsNext int         // Encoders Round Robin next index
	decsNext int         // Decoders Round Robin next index

	// partition and sortGather specific variables
	SortingCols []SortingCol // columns to sort by

	// partition specific variables
	PartitionCols []int                  // column indexes to use for partitioning
	hashRing      *consistent.Consistent // hash ring for consistent hashing
	encsByKey     map[string]encoder     // encoders mapped by key (node address)

	// sortGather specific variables
	batches        []Dataset // current batch from each peer
	batchesNextIdx []int     // next index to visit for each batch per peer
	nextPeer       int       // next peer to read from
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

	err = ex.init(ctx)
	if err != nil {
		return err
	}

	// receive remote data from peers in a go-routine and pass to out channel. Write
	// all errors to receiversErrs channel when done
	receiversErrs := ex.passRemoteData(out)

	rcvDone := false
	sndDone := false
	defer func() {
		// TODO thisNode, _ := ctx.Value(thisNodeKey).(string)
		// TODO fmt.Println(thisNode, "rec", rcvDone, "send", sndDone, err)
		// in case of cancellation, for loop below stops without sending EOF message
		// to all peers. Therefore other peers will not close connections, hence ex.receive
		// will be blocked forever. This will lead to deadlock as current exchange waits on
		// receiversErrs channel that will not be closed
		if !sndDone {
			// close exchange might take time, so don't block preceding runner
			go drain(inp)

			eofErr := ex.encodeAll(ex.encs, errorMsg)
			if err == nil {
				err = eofErr
			}
			eofErr = ex.encodeAll(ex.encsErr, errorMsg)
			if err == nil {
				err = eofErr
			}
		}

		// wait for all receivers to finish
		for range receiversErrs {
		}
	}()

	// send the local data to the peers, until completion or error. Also listen
	// for the completion of the receive go-routine above. When both sending
	// and receiving is complete, exit. Upon error, exit early
	receiversErrsC := receiversErrs
	for err == nil && (!rcvDone || !sndDone) {
		select {
		case <-ctx.Done():
			return nil

		// send data
		case data, ok := <-inp:
			if !ok {
				// the input is exhausted. Notify peers that we're done sending
				// data (they will use it to stop listening to data from us)
				eofErr := ex.encodeAll(ex.encs, eofMsg)
				if err == nil {
					err = eofErr
				}
				eofErr = ex.encodeAll(ex.encsErr, eofMsg)
				if err == nil {
					err = eofErr
				}
				sndDone = true

				// inp is closed. If we keep iterating, it will infinitely resolve
				// to (nil, true). Null-ify it to block it on the next iteration
				inp = nil
				continue
			}
			err = ex.send(data)

		// receive data, returns first error (or nil if receiversErrs was closed)
		case err = <-receiversErrsC:
			rcvDone = true
		}
	}
	return err
}

// Close closes all open connections
func (ex *exchange) Close() (err error) {
	for _, conn := range ex.conns {
		err1 := conn.Close()
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// init initializes the connections, encoders & decoders
func (ex *exchange) init(ctx context.Context) error {
	if ex.inited {
		// exchanged uses a predefined UID and connection listeners on all of
		// the nodes. Running it again would conflict with the existing UID,
		// leading to de-synchronization between the nodes. Thus it's not
		// currently supported. TODO: reconsider this architecture? Perhaps
		// we can distribute the exchange upon Run()?
		// NOTE that while it's possible to run exchange multiple times locally,
		// it's disabled here to guarantee that runners behave locally as they
		// do distributed
		return fmt.Errorf("exhcnage cannot be Run() more than once")
	}
	ex.inited = true

	allNodes, _ := ctx.Value(allNodesKey).([]string)
	thisNode, _ := ctx.Value(thisNodeKey).(string)
	masterNode, _ := ctx.Value(masterNodeKey).(string)
	dist, _ := ctx.Value(distributerKey).(interface {
		Connect(addr, uid string) (net.Conn, error)
	})

	if dist == nil {
		// no distributer was defined - so it's only running locally. We can
		// short-circuit the whole thing
		allNodes = []string{thisNode}
	}

	ex.encsByKey = make(map[string]encoder)
	ex.hashRing = consistent.New()

	// open a connection to all nodes
	connsMap := make(map[string]net.Conn, len(allNodes))
	var sc *shortCircuit

	targetNodes, errTargetNodes := ex.getTargets(allNodes, masterNode, thisNode)
	for _, node := range targetNodes {
		if node == thisNode {
			sc = newShortCircuit()
			ex.conns = append(ex.conns, sc)
			ex.encs = append(ex.encs, sc)
			ex.hashRing.Add(node)
			ex.encsByKey[node] = sc
			continue
		}

		conn, err := dist.Connect(node, ex.UID)
		if err != nil {
			return err
		}

		connsMap[node] = conn
		ex.conns = append(ex.conns, conn)
		enc := gob.NewEncoder(conn)
		ex.encs = append(ex.encs, enc)
		ex.hashRing.Add(node)
		ex.encsByKey[node] = enc
	}
	isTarget := sc != nil
	for _, node := range errTargetNodes {
		if node == thisNode {
			sc = newShortCircuit()
			ex.conns = append(ex.conns, sc)
			ex.encs = append(ex.encs, sc)
			ex.hashRing.Add(node)
			ex.encsByKey[node] = sc
			continue
		}

		conn, err := dist.Connect(node, ex.UID)
		if err != nil {
			return err
		}

		connsMap[node] = conn
		ex.conns = append(ex.conns, conn)
		enc := gob.NewEncoder(conn)
		ex.encsErr = append(ex.encsErr, enc)
	}

	sourceNodes, errSourceNodes := ex.getSources(allNodes, isTarget)
	for _, node := range sourceNodes {
		if node == thisNode {
			ex.decs = append(ex.decs, sc)
			continue
		}

		msg := "THIS " + thisNode + " OTHER " + node

		// we already established a connection to this node from the targets, so we can
		// re-use it. We don't need 2 uni-directional connections
		ex.decs = append(ex.decs, dbgDecoder{gob.NewDecoder(connsMap[node]), msg})
	}
	for _, node := range errSourceNodes {
		if node == thisNode {
			ex.decsErr = append(ex.decsErr, sc)
			continue
		}

		msg := "ERR THIS " + thisNode + " OTHER " + node

		// we already established a connection to this node from the targets, so we can
		// re-use it. We don't need 2 uni-directional connections
		ex.decsErr = append(ex.decsErr, dbgDecoder{gob.NewDecoder(connsMap[node]), msg})
	}
	return nil
}

func (ex *exchange) getTargets(allNodes []string, masterNode, thisNode string) (target, errTarget []string) {
	switch ex.Type {
	case gather, sortGather:
		target = []string{masterNode}
		errTarget = remove(allNodes, masterNode)
	default:
		target = allNodes
	}
	return target, errTarget
}

func (ex *exchange) getSources(allNodes []string, isDest bool) ([]string, []string) {
	// if we're also a destination, listen to all nodes
	if isDest {
		return allNodes, nil
	}
	// otherwise - listen only to errors from all nodes
	return nil, allNodes
}

func remove(list []string, toRemove string) []string {
	for i, s := range list {
		if s == toRemove {
			res := make([]string, len(list)-1)
			copy(res, list[:i])
			copy(res[i:], list[i+1:])
			return res
		}
	}
	return list
}

func (ex *exchange) passRemoteData(out chan Dataset) chan error {
	receiversErrs := make(chan error, len(ex.decsErr)+len(ex.decs))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			data, recErr := ex.receive()
			if recErr == io.EOF {
				return
			}
			if recErr != nil {
				receiversErrs <- recErr
				return
			}
			out <- data
		}
	}()
	for _, d := range ex.decsErr {
		wg.Add(1)
		go func(d decoder) {
			req := &req{}
			err := d.Decode(req)
			if err != nil && err != io.EOF {
				receiversErrs <- err
			}
			wg.Done()
		}(d)
	}
	go func() {
		wg.Wait()
		close(receiversErrs)
	}()
	return receiversErrs
}

// send sends a dataset to destination nodes
func (ex *exchange) send(data Dataset) error {
	switch ex.Type {
	case scatter:
		return ex.encodeScatter(data)
	case partition:
		return ex.encodePartition(data)
	default:
		return ex.encodeAll(ex.encs, data)
	}
}

// encodeAll encodes an object to all destination connections
// expecting e to be either dataset or EOF error
func (ex *exchange) encodeAll(targets []encoder, e interface{}) (err error) {
	req := &req{e}
	for _, enc := range targets {
		err1 := enc.Encode(req)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// receive receives a dataset from next source node
func (ex *exchange) receive() (Dataset, error) {
	switch ex.Type {
	case sortGather:
		return ex.decodeNextSort()
	default:
		return ex.decodeNext()
	}
}

// decodeNext decodes an object from the next source connection in a round robin
func (ex *exchange) decodeNext() (Dataset, error) {
	// if this node is not a receiver or done, return immediately
	if len(ex.decs) == 0 {
		return nil, io.EOF
	}

	i := (ex.decsNext + 1) % len(ex.decs)

	data, err := ex.decodeFrom(i)
	if err == io.EOF {
		// remove the current decoder and try again
		ex.decs = append(ex.decs[:i], ex.decs[i+1:]...)
		return ex.decodeNext()
	}

	ex.decsNext = i
	return data, err
}

// decodeFrom decodes an object from the i-th source connection
func (ex *exchange) decodeFrom(i int) (Dataset, error) {
	req := &req{}
	err := ex.decs[i].Decode(req)
	if err != nil {
		return nil, err
	}
	return req.Payload.(Dataset), nil
}

// interfaces for gob.Encoder/Decoder. Used to also implement the short-circuit.
type encoder interface {
	Encode(interface{}) error
}
type decoder interface {
	Decode(interface{}) error
}

type dbgDecoder struct {
	decoder
	msg string
}

func (dec dbgDecoder) Decode(e interface{}) error {
	// fmt.Println("DECODE", dec.msg)
	err := dec.decoder.Decode(e)
	// fmt.Println("DECODE DONE", dec.msg, e, err)
	if isEOFError(e) {
		return io.EOF
	}
	if isPeerError(e) {
		return errOnPeer
	}
	return err
}

// shortCircuit implements io.Closer, encoder and decoder and provides the
// means to short-circuit internal communications within the same node. This is
// in order to not complicate the generic nature of the exchange code
type shortCircuit struct {
	C      chan interface{}
	closed bool
}

func newShortCircuit() *shortCircuit {
	return &shortCircuit{C: make(chan interface{}, 1000)}
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
	if isPeerError(v) {
		return errOnPeer
	}
	*e.(*req) = *v.(*req)
	return nil
}

func isEOFError(data interface{}) bool {
	err, isErr := data.(*req).Payload.(error)
	return isErr && err.Error() == eofMsg.Error()
}

func isPeerError(data interface{}) bool {
	err, isErr := data.(*req).Payload.(error)
	return isErr && err.Error() == errorMsg.Error()
}
