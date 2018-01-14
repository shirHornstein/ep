package ep

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// MagicNumber used by the built-in Distributer to prefix all of its connections
// It can be used for routing connections
var MagicNumber = []byte("EP01")

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
func NewDistributer(addr string, listener net.Listener) Distributer {
	connsMap := make(map[string]chan net.Conn)
	closeCh := make(chan error, 1)
	d := &distributer{listener, addr, connsMap, &sync.Mutex{}, closeCh}
	go d.start()
	return d
}

type distributer struct {
	listener net.Listener
	addr     string
	connsMap map[string]chan net.Conn
	l        sync.Locker
	closeCh  chan error
}

func (d *distributer) start() error {
	defer close(d.closeCh)
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

	// wait for start() above to exit. otherwise, attempts to re-bind to the
	// same address will infrequently fail with "bind: address already in use".
	// because while the listener is closed, there's still one pending Accept()
	// TODO: consider waiting for all served connections/runners?
	<-d.closeCh
	return err
}

func (d *distributer) Dial(network, addr string) (conn net.Conn, err error) {
	if d.closeCh == nil {
		return nil, io.ErrClosedPipe
	}

	dialer, ok := d.listener.(dialer)
	if ok {
		conn, err = dialer.Dial(network, addr)
	} else {
		conn, err = net.Dial(network, addr)
	}

	if err != nil {
		return
	}

	_, err = conn.Write(MagicNumber)
	if err != nil {
		conn.Close()
	}

	return
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
		conn, err = d.Dial("tcp", addr)
		if err != nil {
			return
		}

		err = writeStr(conn, "D") // Data connection
		if err != nil {
			return
		}

		err = writeStr(conn, d.addr+":"+uid)
		if err != nil {
			return
		}
	} else {
		// listen, timeout after 1 second
		timer := time.NewTimer(time.Second)
		defer timer.Stop()

		select {
		case conn = <-d.connCh(addr + ":" + uid):
			// let it through
		case <-timer.C:
			err = fmt.Errorf("ep: connect timeout; no incoming conn")
		}
	}

	return conn, err
}

func (d *distributer) Serve(conn net.Conn) error {
	prefix := make([]byte, 4)
	_, err := io.ReadFull(conn, prefix)
	if err != nil {
		return err
	}

	if string(prefix) != string(MagicNumber) {
		return fmt.Errorf("unrecognized connection. Missing MagicNumber prefix")
	}

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
	} else if typee == "X" { // execute runner connection
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
		go func() {
			for range out {
			}
		}()

		inp := make(chan Dataset, 1)
		close(inp)

		err = r.Run(context.Background(), inp, out)
		// fmt.Printf("done running with err %+v\n", err)
		if err != nil {
			err = &errMsg{err.Error()}
		}

		// report back to master - either local error or nil payload
		enc := gob.NewEncoder(conn)
		err = enc.Encode(&req{err})
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

func (d *distributer) connCh(k string) chan net.Conn {
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
	Addrs      []string // participating node addresses
	MasterAddr string   // the master node that created the distRunner
	d          *distributer
}

func (r *distRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	errs := []error{}

	decs := []*gob.Decoder{}
	isMain := r.d.addr == r.MasterAddr
	for i := 0; i < len(r.Addrs) && isMain; i++ {
		addr := r.Addrs[i]
		if addr == r.d.addr {
			continue
		}

		var conn net.Conn
		conn, err := r.d.Dial("tcp", addr)
		if err != nil {
			errs = append(errs, err)
			break
		}

		defer conn.Close()
		err = writeStr(conn, "X") // runner connection
		if err != nil {
			errs = append(errs, err)
			break
		}

		enc := gob.NewEncoder(conn)
		err = enc.Encode(r)
		if err != nil {
			errs = append(errs, err)
			break
		}

		decs = append(decs, gob.NewDecoder(conn))
	}

	ctx = context.WithValue(ctx, allNodesKey, r.Addrs)
	ctx = context.WithValue(ctx, masterNodeKey, r.MasterAddr)
	ctx = context.WithValue(ctx, thisNodeKey, r.d.addr)
	ctx = context.WithValue(ctx, distributerKey, r.d)

	// start running query iff no errors were detected
	if len(errs) == 0 {
		err := r.Runner.Run(ctx, inp, out)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// collect error responses
	// The final error is transmitted by the Distributer at the end of the remote
	// Run. We need the top-level runner here to make sure we wait for all runners
	// to complete, thus not leaving any open resources/goroutines
	// note decs contains only peers that successfully got distRunner
	for _, dec := range decs {
		req := &req{}
		err := dec.Decode(req)
		data := req.Payload
		if err == nil {
			err, _ = data.(error)
		}

		if err != nil {
			errs = append(errs, err)
		}
	}

	// return the first error encountered
	if len(errs) > 0 {
		return errs[0]
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
