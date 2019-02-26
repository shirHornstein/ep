package ep

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/panoplyio/epsilon/log"
	"github.com/panoplyio/go-consistent"
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

	inited          bool        // was this runner initialized
	encs            []encoder   // encoders to all destination connections
	decs            []decoder   // decoders from all source connections
	encsTermination []encoder   // encoders to all peers to propagate termination status
	decsTermination []decoder   // decoders from all peers to propagate termination status
	conns           []io.Closer // all open connections (used for closing)
	encsNext        int         // Encoders Round Robin next index
	decsNext        int         // Decoders Round Robin next index

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
		fmt.Println("************ exit exchange ", ex.Type)
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
		// in case of cancellation, for loop below stops without sending EOF message
		// to all peers. Therefore other peers will not close connections, hence ex.receive
		// will be blocked forever. This will lead to deadlock as current exchange waits on
		// receiversErrs channel that will not be closed
		if !sndDone {
			// close exchange might take time, so don't block preceding runner
			go drain(inp)

			msg := errorMsg
			errOnCtx := getError(ctx)
			if errOnCtx == ErrIgnorable {
				msg = eofMsg
			}
			encodeErr := ex.notifyTermination(ctx, msg)
			if err == nil {
				err = encodeErr
			}
		}

		// wait for all receivers to finish
		fmt.Println("exhange before receiversErrs ", ex.Type)
		for range receiversErrs {
		}
		fmt.Println("exhange after receiversErrs ", ex.Type)

	}()

	// send the local data to the peers, until completion or error. Also listen
	// for the completion of the receive go-routine above. When both sending
	// and receiving is complete, exit. Upon error, exit early
	for err == nil && (!rcvDone || !sndDone) {
		select {
		case <-ctx.Done():
			return nil

		// send data
		case data, open := <-inp:
			err = getError(ctx)
			if err != nil && err != ErrIgnorable {
				return nil
			}
			if !open {
				// the input is exhausted. Notify peers that we're done sending
				// data (they will use it to stop listening to data from this node)
				err = ex.notifyTermination(ctx, eofMsg)
				sndDone = true

				// inp is closed. If we keep iterating, it will infinitely resolve
				// to (nil, true). Null-ify it to block it on the next iteration
				inp = nil
				continue
			}
			err = ex.send(data)

		// receive data, returns first error (or nil if receiversErrs was closed)
		case err = <-receiversErrs:
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

	targetNodes, notTargetNodes := ex.getTargetPeers(allNodes, masterNode, thisNode)
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
	for _, node := range notTargetNodes {
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
		ex.encsTermination = append(ex.encsTermination, enc)
	}

	sourceNodes, notSourceNodes := ex.getSourcePeers(allNodes, isTarget)
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
	for _, node := range notSourceNodes {
		if node == thisNode {
			ex.decsTermination = append(ex.decsTermination, sc)
			continue
		}

		msg := "ERR THIS " + thisNode + " OTHER " + node

		// we already established a connection to this node from the targets, so we can
		// re-use it. We don't need 2 uni-directional connections
		ex.decsTermination = append(ex.decsTermination, dbgDecoder{gob.NewDecoder(connsMap[node]), msg})
	}
	return nil
}

func (ex *exchange) getTargetPeers(allNodes []string, masterNode, thisNode string) (target, notTarget []string) {
	switch ex.Type {
	case gather, sortGather:
		target = []string{masterNode}
		notTarget = remove(allNodes, masterNode)
	default:
		target = allNodes
	}
	return target, notTarget
}

func (ex *exchange) getSourcePeers(allNodes []string, isDest bool) (source, notSource []string) {
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
	receiversErrs := make(chan error, len(ex.decsTermination)+len(ex.decs))
	var wg sync.WaitGroup

	// first go routine listens to all ex.decs and stream the data to out channel
	wg.Add(1)
	go func() {
		for {
			data, recErr := ex.receive()
			if recErr == io.EOF { // todo
				fmt.Println("***********************0 recive")
				log.Error("exchange", fmt.Sprintf("exchange type %v", ex.Type), recErr)
				wg.Done()
				return
			}

			if recErr != nil {
				fmt.Println("***********************1 recive")
				log.Error("exchange", fmt.Sprintf("exchange type %v", ex.Type), recErr)
				receiversErrs <- recErr
				continue
			}
			out <- data
		}
	}()

	// next go routines listen to all ex.decsTermination to collect termination
	// statuses from all peers
	wg.Add(len(ex.decsTermination))
	for i, d := range ex.decsTermination {
		go func(d decoder, i int) {
			_, err := decode(d)
			if err != nil && err != io.EOF {
				receiversErrs <- err
			}
			fmt.Println("***********************2 recive")
			log.Error("exchange", fmt.Sprintf("error in peer %d exchange type %v", i, ex.Type), err)
			wg.Done()
		}(d, i)
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
// expecting e to be either dataset or errMsg
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

func (ex *exchange) notifyTermination(ctx context.Context, msg *errMsg) error {
	errEncs := ex.encodeAll(ex.encs, msg)
	errEncsTermination := ex.encodeAll(ex.encsTermination, msg)
	if errEncs != nil {
		return errEncs
	}
	fmt.Println("*********************** notify")
	log.Error("exchange", fmt.Sprintf("notify termination in exchange type %v", ex.Type), msg)
	return errEncsTermination
}

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
	if err != nil {
		// remove the current decoder
		ex.decs = append(ex.decs[:i], ex.decs[i+1:]...)
	} else {
		ex.decsNext = i
	}

	if err == io.EOF {
		// try again from next decoder
		return ex.decodeNext()
	}
	return data, err
}

func (ex *exchange) decodeFrom(i int) (Dataset, error) {
	return decode(ex.decs[i])
}

func decode(d decoder) (Dataset, error) {
	req := &req{}
	err := d.Decode(req)
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
	if isEOFError(e) {
		fmt.Println("DECODE DONE", dec.msg, "EOF")
		return io.EOF
	}
	if isPeerError(e) {
		fmt.Println("DECODE DONE", dec.msg, "E")
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
		fmt.Println("SC: Decode EOF")
		return io.EOF
	}
	if isPeerError(v) {
		fmt.Println("SC: Decode E")
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
