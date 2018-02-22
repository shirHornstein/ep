package ep

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"net"
	"strings"
	"testing"
	"time"
)

// Example of Scatter with just 2 nodes. The datasets are scattered in
// round-robin to the two nodes such that each node receives half of the
// datasets. Thus the output in the local node just returns half of the output.
// Pipelining into a Gather runner would recollected the scattered outputs
func ExampleScatter() {
	ln1, _ := net.Listen("tcp", ":5551")
	dist1 := NewDistributer(":5551", ln1)
	defer dist1.Close()

	ln2, _ := net.Listen("tcp", ":5552")
	dist2 := NewDistributer(":5552", ln2)
	defer dist2.Close()

	runner := dist1.Distribute(Scatter(), ":5551", ":5552")

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)
	fmt.Println(data, err) // no gather - only one batch should return

	// Output: [[foo bar]] <nil>
}

func SkipTestExchangeDialingError(t *testing.T) { //todo: test not stable
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := mockPeer(t, port1)
	defer dist1.Close()

	port2 := ":5552"
	defer mockErrorPeer(t, port2).Close()

	port3 := ":5553"
	defer mockPeer(t, port3).Close()

	runner := dist1.Distribute(Scatter(), port1, port2, port3)

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)

	require.Error(t, err)

	possibleErrors := []string{
		// reported by 5551, starting with "write/read tcp 127.0.0.1:xxx->127.0.0.1:555x"
		": write: broken pipe",       // when dialing to peers
		": connection reset by peer", // when interacting with peers after peers failure

		"bad connection",                        // reported by 5552, when dialing to :5553
		"ep: connect timeout; no incoming conn", // reported by 5553, when waiting to :5552
	}
	errMsg := err.Error()
	isExpectedError := strings.Contains(errMsg, possibleErrors[0]) ||
		strings.Contains(errMsg, possibleErrors[1]) ||
		errMsg == possibleErrors[2] ||
		errMsg == possibleErrors[3]
	require.True(t, isExpectedError, "expected \"%s\" to appear in %s", err.Error(), possibleErrors)
	require.Nil(t, data)
}

// Tests the scattering when there's just one node - the whole thing should
// be short-circuited to act as a pass-through
func TestScatter_singleNode(t *testing.T) {
	port := ":5551"
	dist := mockPeer(t, port)
	defer dist.Close()

	runner := dist.Distribute(Scatter(), port)

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)
	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, 4, data.Len())
}

// Tests the scattering when there's just one node - the whole thing should
// be short-circuited to act as a pass-through
func TestExchangeInit_closeConnectionUponError(t *testing.T) {
	port := ":5551"
	dist := mockPeer(t, port)
	defer dist.Close()

	port2 := ":5552"
	defer mockErrorPeer(t, port2).Close()

	ctx := context.WithValue(context.Background(), distributerKey, dist)
	ctx = context.WithValue(ctx, allNodesKey, []string{port, port2, ":5553"})
	ctx = context.WithValue(ctx, masterNodeKey, port)
	ctx = context.WithValue(ctx, thisNodeKey, port)

	exchange := Scatter().(*exchange)
	err := exchange.Init(ctx)

	require.Error(t, err)
	require.Equal(t, 2, len(exchange.conns))
	require.IsType(t, &shortCircuit{}, exchange.conns[0])
	require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
}

func TestScatter_and_Gather(t *testing.T) {
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := mockPeer(t, port1)
	defer dist1.Close()

	port2 := ":5552"
	defer mockPeer(t, port2).Close()

	runner := Pipeline(Scatter(), &nodeAddr{}, Gather())
	runner = dist1.Distribute(runner, port1, port2)

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)

	require.NoError(t, err)
	require.Equal(t, "[[hello world foo bar] [:5552 :5552 :5551 :5551]]", fmt.Sprintf("%v", data))
}

// UID should be unique per generated exchange function
func TestExchange_unique(t *testing.T) {
	s1 := Scatter().(*exchange)
	s2 := Scatter().(*exchange)
	s3 := Gather().(*exchange)
	require.NotEqual(t, s1.UID, s2.UID)
	require.NotEqual(t, s2.UID, s3.UID)
}

var _ = registerGob(&nodeAddr{})

type nodeAddr struct{}

func (*nodeAddr) Returns() []Type { return []Type{Wildcard, str} }
func (*nodeAddr) Run(ctx context.Context, inp, out chan Dataset) error {
	addr := ctx.Value(thisNodeKey).(string)
	for data := range inp {
		res := make(strs, data.Len())
		for i := range res {
			res[i] = addr
		}

		outset := []Data{}
		for i := 0; i < data.Width(); i++ {
			outset = append(outset, data.At(i))
		}

		outset = append(outset, res)
		out <- NewDataset(outset...)
	}
	return nil
}

func mockPeer(t *testing.T, port string) Distributer {
	ln, err := net.Listen("tcp", port)
	require.NoError(t, err)
	return NewDistributer(port, ln)
}

func mockErrorPeer(t *testing.T, port string) Distributer {
	ln, err := net.Listen("tcp", port)
	require.NoError(t, err)
	dialer := &errDialer{ln, fmt.Errorf("bad connection")}
	return NewDistributer(port, dialer)
}

type errDialer struct {
	net.Listener
	Err error
}

func (e *errDialer) Dial(net, addr string) (net.Conn, error) {
	return nil, e.Err
}
