package ep

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"testing"
	"time"
)

var _ = Runners.Register("cancel", &canceler{})

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
	port, port2 := ":5551", ":5552"
	distributers := startCluster(t, port, port2)
	master := distributers[0]

	defer func() {
		for _, d := range distributers {
			require.NoError(t, d.Close())
		}
	}()

	ctx := context.WithValue(context.Background(), distributerKey, master)
	ctx = context.WithValue(ctx, allNodesKey, []string{port, port2})
	ctx = context.WithValue(ctx, masterNodeKey, port)
	ctx = context.WithValue(ctx, thisNodeKey, port)

	for name, ex := range exchanges {
		t.Run(name+"/cancel on master", func(t *testing.T) {
			exchange := ex().(*exchange)
			runner := Pipeline(Project(PassThrough(), PassThrough()), exchange)
			runner = master.Distribute(runner, port, port2)

			inp := make(chan Dataset)
			out := make(chan Dataset)
			defer close(inp)
			defer close(out)
			ctx, cancel := context.WithCancel(ctx)
			cancel()
			err := runner.Run(ctx, inp, out)
			require.NoError(t, err)

			require.Equal(t, 2, len(exchange.conns))
			require.IsType(t, &shortCircuit{}, exchange.conns[0])
			require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
		})

		t.Run(name+"/error on peer", func(t *testing.T) {
			t.Skip("TODO test peer") // TODO Avia

			exchange := ex().(*exchange)
			runner := Pipeline(cancelOnPort(port2), Project(PassThrough(), PassThrough()), exchange)
			runner = master.Distribute(runner, port, port2)

			inp := make(chan Dataset)
			out := make(chan Dataset)
			defer close(inp)
			defer close(out)

			err := runner.Run(ctx, inp, out)
			require.Error(t, err)
			require.Equal(t, "error from :5552", err.Error())

			require.Equal(t, 2, len(exchange.conns))
			require.IsType(t, &shortCircuit{}, exchange.conns[0])
			require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
			require.Contains(t, exchange.conns[1].Close().Error(), "use of closed network connection", "open connections leak")
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

func cancelOnPort(port string) Runner {
	return &canceler{port}
}

type canceler struct {
	Port string
}

func (*canceler) Returns() []Type { return nil }
func (r *canceler) Run(ctx context.Context, inp, out chan Dataset) error {
	if ctx.Value(thisNodeKey).(string) == r.Port {
		return fmt.Errorf("error from %s", r.Port)
	}
	for range inp {
	}
	return nil
}
