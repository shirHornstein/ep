package ep

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/panoplyio/go-consistent"
	"github.com/satori/go.uuid"
	"io"
	"net"
	"strings"
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

// Gather returns an exchange Runner that gathers all of its input into a
// single node. On the main node it will passThrough data from all other
// nodes, and will produce no output on peers
func Gather() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: gather}
}

// Scatter returns an exchange Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a round-
// robin to the nodes
func Scatter() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: scatter}
}

// Broadcast returns an exchange Runner that duplicates its input to all
// other nodes. The output will be effectively a union of all inputs from
// all nodes. Order not guaranteed
func Broadcast() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: broadcast}
}

// Partition returns an exchange Runner that routes the data between nodes using
// consistent hashing algorithm. The provided column of an incoming dataset
// will be used to find an appropriate endpoint for this data. Order not guaranteed
func Partition(columns ...int) Runner {
	uid, _ := uuid.NewV4()

	sortCols := make([]SortingCol, len(columns))
	for i := 0; i < len(sortCols); i++ {
		sortCols[i] = SortingCol{Index: columns[i]}
	}

	return &exchange{UID: uid.String(), Type: partition, PartitionCols: sortCols}
}

// exchange is a Runner that exchanges data between peer nodes
type exchange struct {
	UID  string
	Type exchangeType

	inited   bool        // was this runner initialized
	encs     []encoder   // encoders to all destination connections
	decs     []decoder   // decoders from all source connections
	conns    []io.Closer // all open connections (used for closing)
	encsNext int         // Encoders Round Robin next index
	decsNext int         // Decoders Round Robin next index

	// partition specific variables
	PartitionCols []SortingCol           // column indexes to use for partitioning
	hashRing      *consistent.Consistent // hash ring for consistent hashing
	encsByKey     map[string]encoder     // encoders mapped by key (node address)

	// sortGather specific variables
	SortingCols    []SortingCol // columns to sort by
	batches        []Dataset    // current batch from each peer
	batchesNextIdx []int        // next index to visit for each batch per peer
	nextPeer       int          // next peer to read from
}

func (ex *exchange) Returns() []Type { return []Type{Wildcard} }
func (ex *exchange) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	if ex.inited {
		// exchanged uses a predefined UID and connection listeners on all of
		// the nodes. Running it again would conflict with the existing UID,
		// leading to de-synchronization between the nodes. Thus it's not
		// currently supported. TODO: reconsider this architecture? Perhaps
		// we can distribute the exchange upon Run()?
		// NOTE that while it's possible to run exchange multiple times locally,
		// it's disabled here to guarantee that runners behave locally as they
		// do distributed.
		return fmt.Errorf("exhcnage cannot be Run() more than once")
	}

	ex.inited = true
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

	// receive remote data from peers in a go-routine. Write the final error (or
	// nil) to the channel when done.
	errs := make(chan error)
	go func() {
		defer close(errs)
		for {
			data, recErr := ex.receive()
			if recErr == io.EOF {
				errs <- nil
				break
			}
			if recErr != nil {
				errs <- recErr
				return
			}
			out <- data
		}
	}()

	// send the local data to the peers, until completion or error. Also listen
	// for the completion of the received go-routine above. When both sending
	// and receiving is complete, exit. Upon error, exit early.
	rcvDone := false
	sndDone := false
	defer func() {
		// in case of cancellation, select below stops without sending EOF message
		// to all peers. Therefore other peers will not close connections, hence ex.receive
		// will be blocked forever. This will lead to deadlock as current exchange waits on
		// errs channel that will not be closed
		if !sndDone {
			eofMsg := &errMsg{io.EOF.Error()}
			ex.encodeAll(eofMsg)
		}

		// wait for all receivers to finish
		for range errs {
		}
	}()
	for err == nil && (!rcvDone || !sndDone) {
		select {
		case data, ok := <-inp:
			if !ok {
				// the input is exhausted. Notify peers that we're done sending
				// data (they will use it to stop listening to data from us).
				eofMsg := &errMsg{io.EOF.Error()}
				ex.encodeAll(eofMsg)
				sndDone = true

				// inp is closed. If we keep iterating, it will infinitely
				// resolve to (nil, true). Null-ify it to block it on the next
				// iteration.
				inp = nil
				continue
			}

			err = ex.send(data)
		case err = <-errs:
			rcvDone = true // errors (or nil) from the receive go-routine
		case <-ctx.Done(): // context timeout or cancel
			err = ctx.Err()
			// as all other runners - in case of cancellation, runner should stop
			// without effecting final error
			if err == context.Canceled {
				return nil
			}
		}
	}
	return err
}

// send sends a dataset to destination nodes
func (ex *exchange) send(data Dataset) error {
	switch ex.Type {
	case scatter:
		return ex.encodeNext(data)
	case partition:
		return ex.encodePartition(data)
	default:
		return ex.encodeAll(data)
	}
}

// receive receives a dataset from next source node
func (ex *exchange) receive() (Dataset, error) {
	// if this node is not a receiver or done, return immediately
	if len(ex.decs) == 0 {
		return nil, io.EOF
	}

	switch ex.Type {
	case sortGather:
		return ex.decodeNextSort()
	default:
		return ex.decodeNext()
	}
}

// decodeNext decodes an object from the next source connection in a round robin
func (ex *exchange) decodeNext() (Dataset, error) {
	if len(ex.decs) == 0 {
		return nil, io.EOF
	}

	i := (ex.decsNext + 1) % len(ex.decs)

	data, err := ex.decodeFrom(i)
	if err == io.EOF {
		// remove the current decoder and try again
		ex.decs = append(ex.decs[:i], ex.decs[i+1:]...)
		return ex.receive()
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

// encodeAll encodes an object to all destination connections
// expecting e to be either dataset or EOF error
func (ex *exchange) encodeAll(e interface{}) (err error) {
	req := &req{e}
	for _, enc := range ex.encs {
		err1 := enc.Encode(req)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// encodeNext encodes an object to the next destination connection in a round robin
func (ex *exchange) encodeNext(e interface{}) error {
	if len(ex.encs) == 0 {
		return io.ErrClosedPipe
	}

	req := &req{e}
	ex.encsNext = (ex.encsNext + 1) % len(ex.encs)
	return ex.encs[ex.encsNext].Encode(req)
}

// encodePartition encodes an object to a destination connection selected by partitioning
func (ex *exchange) encodePartition(e interface{}) error {
	data, ok := e.(Dataset)
	if !ok {
		return fmt.Errorf("encodePartition called without a dataset")
	}

	// before partitioning data, sort it to generate larger batches
	Sort(data, ex.PartitionCols)
	stringValues := data.ColumnStrings()

	lastSeenHash := ex.getRowHash(stringValues, 0)
	lastSlicedRow := 0
	for row := 1; row < data.Len(); row++ {
		hash := ex.getRowHash(stringValues, row)
		if hash == lastSeenHash {
			continue
		}

		dataToEncode := data.Slice(lastSlicedRow, row)
		err := ex.partitionData(dataToEncode, lastSeenHash)
		if err != nil {
			return err
		}

		lastSeenHash = hash
		lastSlicedRow = row
	}

	// leftover
	dataToEncode := data.Slice(lastSlicedRow, data.Len())
	return ex.partitionData(dataToEncode, lastSeenHash)
}

func (ex *exchange) getRowHash(stringValues [][]string, row int) string {
	var sb strings.Builder
	for _, col := range ex.PartitionCols {
		sb.WriteString(stringValues[col.Index][row])
	}
	return sb.String()
}

func (ex *exchange) partitionData(data Data, hash string) error {
	enc, err := ex.getPartitionEncoder(hash)
	if err != nil {
		return err
	}

	return enc.Encode(&req{data})
}

// getPartitionEncoder uses a hash ring to find a node that should handle
// a provided key. This function returns an encoder that handles data
// transmission to the matched node.
func (ex *exchange) getPartitionEncoder(key string) (encoder, error) {
	endpoint, err := ex.hashRing.Get(key)
	if err != nil {
		return nil, fmt.Errorf("cannot find a target node: %s", err)
	}

	enc, ok := ex.encsByKey[endpoint]
	if !ok {
		return nil, fmt.Errorf("no matching node found")
	}

	return enc, nil
}

// init initializes the connections, encoders & decoders
func (ex *exchange) init(ctx context.Context) (err error) {
	// hashRing handles partitioning between nodes
	ex.hashRing = consistent.New()

	// Partitioning assigns datasets to string addresses of nodes,
	// while only encoders can actually send data.
	// By using a map we can find an encoder for every address
	ex.encsByKey = make(map[string]encoder)

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

	targetNodes := allNodes
	if ex.Type == gather || ex.Type == sortGather {
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
	for _, node := range targetNodes {
		if node == thisNode {
			shortCircuit = newShortCircuit()
			ex.conns = append(ex.conns, shortCircuit)
			ex.encs = append(ex.encs, shortCircuit)
			ex.hashRing.Add(node)
			ex.encsByKey[node] = shortCircuit
			continue
		}

		conn, err = dist.Connect(node, ex.UID)
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
