package ep

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep/compare"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"testing"
	"time"
)

var _ = Runners.
	Register("err", &errOnPort{}).
	Register("cancel", &waitForCancel{}).
	Register("delayedCancel", &delayedCancel{}).
	Register("drainInp", &drainInp{}).
	Register("fixedData", &fixedData{})

var _ = Types.Register("dummyString", str)
var str = &strType{}

var exchanges = map[string]func() Runner{
	"gather":     Gather,
	"sortGather": func() Runner { return SortGather(nil) },
	"scatter":    Scatter,
	"broadcast":  Broadcast,
	"partition":  func() Runner { return Partition(0) },
}

func TestExchange_uniqueUIDPerExchanger(t *testing.T) {
	s1 := Scatter().(*exchange)
	s2 := Scatter().(*exchange)
	s3 := Gather().(*exchange)
	require.NotEqual(t, s1.UID, s2.UID)
	require.NotEqual(t, s2.UID, s3.UID)
}

func TestExchange_init_createEncsDecsToAll(t *testing.T) {
	ports := []string{":5551", ":5552", ":5553", ":5554"}
	master := ports[0]
	peer := ports[2]

	for name, ex := range exchanges {
		exchange := ex().(*exchange)
		t.Run("targets from master/"+name, func(t *testing.T) {
			targets, errTargets := exchange.getTargets(ports, master, master)
			require.Equal(t, len(ports), len(targets)+len(errTargets))
		})

		t.Run("targets from peer/"+name, func(t *testing.T) {
			targets, errTargets := exchange.getTargets(ports, master, peer)
			require.Equal(t, len(ports), len(targets)+len(errTargets))
		})

		t.Run("source/"+name, func(t *testing.T) {
			sources, errSources := exchange.getSources(ports, true)
			require.Equal(t, len(ports), len(sources)+len(errSources))

			sources, errSources = exchange.getSources(ports, false)
			require.Equal(t, len(ports), len(sources)+len(errSources))
		})
	}
}

func TestExchange_init_closeAllConnectionsUponError(t *testing.T) {
	port := ":5551"
	ln, err := net.Listen("tcp", port)
	require.NoError(t, err)
	dist := NewDistributer(port, ln)

	port2 := ":5552"

	ctx := context.WithValue(context.Background(), distributerKey, dist)
	ctx = context.WithValue(ctx, allNodesKey, []string{port, port2})
	ctx = context.WithValue(ctx, masterNodeKey, port)
	ctx = context.WithValue(ctx, thisNodeKey, port)

	exchange := Scatter().(*exchange)
	err = exchange.init(ctx)

	require.Error(t, err)
	require.Equal(t, "dial tcp :5552: connect: connection refused", err.Error())
	require.Equal(t, 1, len(exchange.conns))
	require.IsType(t, &shortCircuit{}, exchange.conns[0])
	require.NoError(t, exchange.Close())
	require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
	require.NoError(t, dist.Close())
}

func TestExchange_error(t *testing.T) {
	ports := []string{":5551", ":5552"}
	distributers := startCluster(t, ports...)
	master := distributers[0]

	defer func() {
		for _, d := range distributers {
			require.NoError(t, d.Close())
		}
	}()

	errorWhileReadingFromInp := func(t *testing.T, ex func() Runner, cancelOnMaster bool) {
		cancelOnPort := ports[1]
		if cancelOnMaster {
			cancelOnPort = ports[0]
		}

		exchange := ex().(*exchange)
		runner := Pipeline(&fixedData{}, &errOnPort{cancelOnPort}, &waitForCancel{}, exchange, &drainInp{})
		runner = master.Distribute(runner, ports...)

		inp := make(chan Dataset)
		out := make(chan Dataset)
		close(inp)
		defer close(out)

		err := runner.Run(context.Background(), inp, out)
		require.Error(t, err)
		require.Equal(t, "error from "+cancelOnPort, err.Error())

		require.Equal(t, 2, len(exchange.conns))
		require.IsType(t, &shortCircuit{}, exchange.conns[0])
		require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
		require.Contains(t, exchange.conns[1].Close().Error(), "use of closed network connection", "open connections leak")
	}

	errorAfterReadingFromInpDone := func(t *testing.T, ex func() Runner, cancelOnMaster bool) {
		cancelOnPort := ports[1]
		if cancelOnMaster {
			cancelOnPort = ports[0]
		}

		exchange := ex().(*exchange)
		runner := Pipeline(&errOnPort{cancelOnPort}, delayCancel(exchange, cancelOnPort))
		runner = master.Distribute(runner, ports...)

		inp := make(chan Dataset)
		out := make(chan Dataset)
		close(inp)
		defer close(out)

		err := runner.Run(context.Background(), inp, out)
		require.Error(t, err)
		require.Equal(t, "error from "+cancelOnPort, err.Error())

		require.Equal(t, 2, len(exchange.conns))
		require.IsType(t, &shortCircuit{}, exchange.conns[0])
		require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
		require.Contains(t, exchange.conns[1].Close().Error(), "use of closed network connection", "open connections leak")
	}

	for name, ex := range exchanges {
		t.Run(name+"/error on master/inp open", func(t *testing.T) {
			errorWhileReadingFromInp(t, ex, true)
		})

		t.Run(name+"/error on peer/inp open", func(t *testing.T) {
			errorWhileReadingFromInp(t, ex, false)
		})

		t.Run(name+"/error on master/inp close", func(t *testing.T) {
			errorAfterReadingFromInpDone(t, ex, true)
		})

		t.Run(name+"/error on peer/inp close", func(t *testing.T) {
			errorAfterReadingFromInpDone(t, ex, false)
		})
	}
}

func TestPartition_addsMembersToHashRing(t *testing.T) {
	port1 := ":5551"
	ln, err := net.Listen("tcp", port1)
	require.NoError(t, err)
	peer1 := NewDistributer(port1, ln)

	port2 := ":5552"
	ln, err = net.Listen("tcp", port2)
	require.NoError(t, err)
	peer2 := NewDistributer(port2, ln)

	port3 := ":5553"
	ln, err = net.Listen("tcp", port3)
	require.NoError(t, err)
	peer3 := NewDistributer(port3, ln)

	defer func() {
		require.NoError(t, peer1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	allNodes := []string{port1, port2, port3}
	ctx := context.WithValue(context.Background(), distributerKey, peer1)
	ctx = context.WithValue(ctx, allNodesKey, allNodes)
	ctx = context.WithValue(ctx, masterNodeKey, port1)
	ctx = context.WithValue(ctx, thisNodeKey, port1)

	partition := Partition(0).(*exchange)
	err = partition.init(ctx)
	require.NoError(t, err)

	members := partition.hashRing.Members()
	require.ElementsMatchf(t, members, allNodes, "%s != %s", members, allNodes)
}

func TestExchange_encodePartition_failsWithoutDataset(t *testing.T) {
	port1 := ":5551"
	ln, err := net.Listen("tcp", port1)
	require.NoError(t, err)
	peer1 := NewDistributer(port1, ln)

	port2 := ":5552"
	ln, err = net.Listen("tcp", port2)
	require.NoError(t, err)
	peer2 := NewDistributer(port2, ln)

	port3 := ":5553"
	ln, err = net.Listen("tcp", port3)
	require.NoError(t, err)
	peer3 := NewDistributer(port3, ln)

	defer func() {
		require.NoError(t, peer1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	ctx := context.WithValue(context.Background(), distributerKey, peer1)
	ctx = context.WithValue(ctx, allNodesKey, []string{port1, port2, port3})
	ctx = context.WithValue(ctx, masterNodeKey, port1)
	ctx = context.WithValue(ctx, thisNodeKey, port1)

	partition := Partition(0).(*exchange)
	err = partition.init(ctx)
	require.NoError(t, err)

	require.Error(t, partition.encodePartition([]int{42}))
}

func TestExchange_getPartitionEncoder_consistent(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	maxPort := 7000
	minPort := 6000
	randomPort := rand.Intn(maxPort-minPort) + minPort

	port1 := fmt.Sprintf(":%d", randomPort)
	ln, err := net.Listen("tcp", port1)
	require.NoError(t, err)
	peer1 := NewDistributer(port1, ln)

	port2 := fmt.Sprintf(":%d", randomPort+1)
	ln, err = net.Listen("tcp", port2)
	require.NoError(t, err)
	peer2 := NewDistributer(port2, ln)

	port3 := fmt.Sprintf(":%d", randomPort+2)
	ln, err = net.Listen("tcp", port3)
	require.NoError(t, err)
	peer3 := NewDistributer(port3, ln)

	defer func() {
		require.NoError(t, peer1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	ctx := context.WithValue(context.Background(), distributerKey, peer1)
	ctx = context.WithValue(ctx, allNodesKey, []string{port1, port2, port3})
	ctx = context.WithValue(ctx, masterNodeKey, port1)
	ctx = context.WithValue(ctx, thisNodeKey, port1)

	partition := Partition(0).(*exchange)
	err = partition.init(ctx)
	require.NoError(t, err)

	hashKey := fmt.Sprintf("this-is-a-key-%d", rand.Intn(1e12))
	enc, err := partition.getPartitionEncoder(hashKey)
	require.NoError(t, err)

	// verify that getPartitionEncoder returns the same result over 100 calls
	for i := 0; i < 100; i++ {
		nextEncoder, err := partition.getPartitionEncoder(hashKey)
		require.NoError(t, err)
		require.Equal(t, enc, nextEncoder)
	}
}

func TestExchange_encsMappedByNodes(t *testing.T) {
	port1 := ":5551"
	ln, err := net.Listen("tcp", port1)
	require.NoError(t, err)
	peer1 := NewDistributer(port1, ln)

	port2 := ":5552"
	ln, err = net.Listen("tcp", port2)
	require.NoError(t, err)
	peer2 := NewDistributer(port2, ln)

	port3 := ":5553"
	ln, err = net.Listen("tcp", port3)
	require.NoError(t, err)
	peer3 := NewDistributer(port3, ln)

	defer func() {
		require.NoError(t, peer1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	allNodes := []string{port1, port2, port3}
	ctx := context.WithValue(context.Background(), distributerKey, peer1)
	ctx = context.WithValue(ctx, allNodesKey, allNodes)
	ctx = context.WithValue(ctx, masterNodeKey, port1)
	ctx = context.WithValue(ctx, thisNodeKey, port1)

	partition := Partition(0).(*exchange)
	err = partition.init(ctx)
	require.NoError(t, err)

	encKeys := make([]string, 0, len(partition.encsByKey))
	for k := range partition.encsByKey {
		encKeys = append(encKeys, k)
	}
	require.ElementsMatch(t, []string{":5551", ":5552", ":5553"}, encKeys)
}

func startCluster(t *testing.T, ports ...string) []Distributer {
	res := make([]Distributer, len(ports))
	for i, port := range ports {
		ln, err := net.Listen("tcp", port)
		require.NoError(t, err)
		res[i] = NewDistributer(port, ln)
	}
	return res
}

type errOnPort struct {
	Port string
}

func (*errOnPort) Returns() []Type { return nil }
func (r *errOnPort) Run(ctx context.Context, inp, out chan Dataset) error {
	if ctx.Value(thisNodeKey).(string) == r.Port {
		return fmt.Errorf("error from %s", r.Port)
	}
	for data := range inp {
		out <- data
	}
	return nil
}

type waitForCancel struct {
	// Name is unused field, defined to allow gob-ing infinityRunner between peers
	Name string
}

func (*waitForCancel) Returns() []Type { return nil }
func (r *waitForCancel) Run(ctx context.Context, inp, out chan Dataset) error {
	go func() {
		for data := range inp {
			out <- data
		}
	}()

	// infinitely wait for cancel
	<-ctx.Done()
	time.Sleep(time.Second)
	return nil
}

func delayCancel(r Runner, port string) Runner {
	return &delayedCancel{r, port}
}

type delayedCancel struct {
	Runner
	Port string
}

func (r *delayedCancel) Returns() []Type { return r.Runner.Returns() }
func (r *delayedCancel) Run(ctx context.Context, inp, out chan Dataset) error {
	if ctx.Value(thisNodeKey).(string) != r.Port {
		return r.Runner.Run(ctx, inp, out)
	}

	internalInp := make(chan Dataset)
	internalOut := make(chan Dataset)
	internalCtx, cancel := context.WithCancel(context.Background())
	internalCtx = context.WithValue(internalCtx, distributerKey, ctx.Value(distributerKey))
	internalCtx = context.WithValue(internalCtx, allNodesKey, ctx.Value(allNodesKey))
	internalCtx = context.WithValue(internalCtx, masterNodeKey, ctx.Value(masterNodeKey))
	internalCtx = context.WithValue(internalCtx, thisNodeKey, ctx.Value(thisNodeKey))

	go func() {
		<-ctx.Done()
		close(internalInp)
		time.Sleep(time.Second)

		cancel()
	}()

	return r.Runner.Run(internalCtx, internalInp, internalOut)
}

type drainInp struct{}

func (*drainInp) Returns() []Type { return nil }
func (r *drainInp) Run(ctx context.Context, inp, out chan Dataset) error {
	for range inp {
	}
	return nil
}

type fixedData struct{}

func (*fixedData) Returns() []Type { return nil }
func (r *fixedData) Run(ctx context.Context, inp, out chan Dataset) error {
	out <- NewDataset(str.Data(2))
	out <- NewDataset(strs{"a", "b"})

	for data := range inp {
		out <- data
	}
	return nil
}

type strType struct{}

func (s *strType) String() string     { return s.Name() }
func (*strType) Name() string         { return "string" }
func (*strType) Size() uint           { return 8 }
func (*strType) Data(n int) Data      { return make(strs, n) }
func (*strType) DataEmpty(n int) Data { return make(strs, 0, n) }

type strs []string

func (strs) Type() Type                                { return str }
func (vs strs) Len() int                               { return len(vs) }
func (vs strs) Less(int, int) bool                     { return false }
func (vs strs) Swap(int, int)                          {}
func (vs strs) LessOther(int, Data, int) bool          { return false }
func (vs strs) Slice(int, int) Data                    { return vs }
func (vs strs) Append(Data) Data                       { return vs }
func (vs strs) Duplicate(t int) Data                   { return vs }
func (vs strs) IsNull(int) bool                        { return false }
func (vs strs) MarkNull(int)                           {}
func (vs strs) Nulls() []bool                          { return make([]bool, vs.Len()) }
func (vs strs) Equal(Data) bool                        { return false }
func (vs strs) Compare(Data) ([]compare.Result, error) { return make([]compare.Result, vs.Len()), nil }
func (vs strs) Copy(Data, int, int)                    {}
func (vs strs) Strings() []string                      { return vs }
