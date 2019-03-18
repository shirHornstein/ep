package ep

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

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
			targets, rest := exchange.getTargetPeers(ports, master, master)
			require.Equal(t, len(ports), len(targets)+len(rest))
		})

		t.Run("targets from peer/"+name, func(t *testing.T) {
			targets, rest := exchange.getTargetPeers(ports, master, peer)
			require.Equal(t, len(ports), len(targets)+len(rest))
		})

		t.Run("source/"+name, func(t *testing.T) {
			sources, rest := exchange.getSourcePeers(ports, true)
			require.Equal(t, len(ports), len(sources)+len(rest))

			sources, rest = exchange.getSourcePeers(ports, false)
			require.Equal(t, len(ports), len(sources)+len(rest))
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
	assert.NoError(t, exchange.Close())
	require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
	assert.NoError(t, dist.Close())
}

func TestExchange_errorSingleNode(t *testing.T) {
	port := ":5551"
	distributers := startCluster(t, port)
	master := distributers[0]

	defer terminateCluster(t, distributers...)

	ctx := context.WithValue(context.Background(), distributerKey, master)
	ctx = context.WithValue(ctx, allNodesKey, []string{port})
	ctx = context.WithValue(ctx, masterNodeKey, port)
	ctx = context.WithValue(ctx, thisNodeKey, port)

	for name, ex := range exchanges {
		t.Run(name+"/error while inp open", func(t *testing.T) {
			exchange := ex().(*exchange)

			inp := make(chan Dataset)
			out := make(chan Dataset)
			defer close(inp)
			defer close(out)
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			err := exchange.Run(ctx, inp, out)
			require.NoError(t, err)

			require.Equal(t, 1, len(exchange.conns))
			require.IsType(t, &shortCircuit{}, exchange.conns[0])
			require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
		})

		t.Run(name+"/error on closed inp", func(t *testing.T) {
			exchange := ex().(*exchange)
			runner := closeWithoutCancel(exchange, port)
			inp := make(chan Dataset)
			out := make(chan Dataset)
			close(inp)
			defer close(out)
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			err := runner.Run(ctx, inp, out)
			require.NoError(t, err)

			require.Equal(t, 1, len(exchange.conns))
			require.IsType(t, &shortCircuit{}, exchange.conns[0])
			require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
		})
	}
}

func TestExchange_errorPropagationAmongPeers(t *testing.T) {
	ports := []string{":5551", ":5552", ":5553"}
	distributers := startCluster(t, ports...)
	master := distributers[0]

	defer terminateCluster(t, distributers...)

	runAndVerify := func(t *testing.T, runner Runner, errors []string, exchanges ...*exchange) {
		inp := make(chan Dataset, 2)
		out := make(chan Dataset)
		inp <- NewDataset(strs{"a", "b"})
		inp <- NewDataset(strs{"c", "d"})
		close(inp)
		defer close(out)

		err := runner.Run(context.Background(), inp, out)
		require.Error(t, err)
		require.Contains(t, errors, err.Error())

		for _, exchange := range exchanges {
			require.Equal(t, len(ports), len(exchange.conns))
			require.IsType(t, &shortCircuit{}, exchange.conns[0])
			require.True(t, (exchange.conns[0]).(*shortCircuit).closed, "open connections leak")
			require.Contains(t, exchange.conns[1].Close().Error(), "use of closed network connection", "open connections leak")
		}
	}

	errorWhileReadingFromInp := func(t *testing.T, ex func() Runner, cancelOnPort string) {
		exchange := ex().(*exchange)
		runner := Pipeline(&fixedData{}, &errOnPort{cancelOnPort}, cancelWithoutClose(exchange, cancelOnPort), &drainInp{})
		runner = master.Distribute(runner, ports...)

		runAndVerify(t, runner, []string{"error from " + cancelOnPort}, exchange)
	}
	errorsWhileReadingFromInp := func(t *testing.T, ex func() Runner, port1, port2 string) {
		exchange1 := ex().(*exchange)
		exchange2 := ex().(*exchange)
		runner := Pipeline(
			&fixedData{},
			&errOnPort{port1}, cancelWithoutClose(exchange1, port1),
			Project(PassThrough(), PassThrough()),
			&errOnPort{port2}, cancelWithoutClose(exchange2, port2),
			&drainInp{},
		)
		runner = master.Distribute(runner, ports...)

		runAndVerify(t, runner, []string{"error from " + port1, "error from " + port2}, exchange1, exchange2)
	}

	errorAfterReadingFromInpDone := func(t *testing.T, ex func() Runner, cancelOnPort string) {
		exchange := ex().(*exchange)
		runner := Pipeline(&fixedData{}, &errOnPort{cancelOnPort}, closeWithoutCancel(exchange, cancelOnPort), &drainInp{})
		runner = master.Distribute(runner, ports...)

		runAndVerify(t, runner, []string{"error from " + cancelOnPort}, exchange)
	}
	errorsAfterReadingFromInpDone := func(t *testing.T, ex func() Runner, port1, port2 string) {
		exchange1 := ex().(*exchange)
		exchange2 := ex().(*exchange)
		runner := Pipeline(
			&fixedData{},
			&errOnPort{port1}, closeWithoutCancel(exchange1, port1),
			Project(PassThrough(), PassThrough()),
			&errOnPort{port2}, closeWithoutCancel(exchange2, port2),
			&drainInp{},
		)
		runner = master.Distribute(runner, ports...)

		runAndVerify(t, runner, []string{"error from " + port1, "error from " + port2}, exchange1, exchange2)
	}

	for name, ex := range exchanges {
		t.Run(name+"/error on master/inp open", func(t *testing.T) {
			errorWhileReadingFromInp(t, ex, ports[0])
		})

		t.Run(name+"/error on peer/inp open", func(t *testing.T) {
			errorWhileReadingFromInp(t, ex, ports[1])
		})

		t.Run(name+"/two errors on master/inp open", func(t *testing.T) {
			errorsWhileReadingFromInp(t, ex, ports[0], ports[0])
		})

		t.Run(name+"/errors on master and peer/inp open", func(t *testing.T) {
			errorsWhileReadingFromInp(t, ex, ports[2], ports[0])
		})

		t.Run(name+"/errors on two peers/inp open", func(t *testing.T) {
			errorsWhileReadingFromInp(t, ex, ports[1], ports[2])
		})

		t.Run(name+"/error on master/inp close", func(t *testing.T) {
			errorAfterReadingFromInpDone(t, ex, ports[0])
		})

		t.Run(name+"/error on peer/inp close", func(t *testing.T) {
			errorAfterReadingFromInpDone(t, ex, ports[1])
		})

		t.Run(name+"/two errors on master/inp close", func(t *testing.T) {
			errorsAfterReadingFromInpDone(t, ex, ports[0], ports[0])
		})

		t.Run(name+"/errors on master and peer/inp close", func(t *testing.T) {
			errorsAfterReadingFromInpDone(t, ex, ports[2], ports[0])
		})

		t.Run(name+"/errors on two peers/inp close", func(t *testing.T) {
			errorsAfterReadingFromInpDone(t, ex, ports[1], ports[2])
		})
	}
}

func TestPartition_addsMembersToHashRing(t *testing.T) {
	allNodes := []string{":5551", ":5552", ":5553"}
	distributers := startCluster(t, allNodes...)
	master := distributers[0]

	defer terminateCluster(t, distributers...)

	ctx := context.WithValue(context.Background(), distributerKey, master)
	ctx = context.WithValue(ctx, allNodesKey, allNodes)
	ctx = context.WithValue(ctx, masterNodeKey, allNodes[0])
	ctx = context.WithValue(ctx, thisNodeKey, allNodes[0])

	partition := Partition(0).(*exchange)
	err := partition.init(ctx)
	require.NoError(t, err)

	members := partition.hashRing.Members()
	require.ElementsMatchf(t, allNodes, members, "%s != %s", members, allNodes)
}

func TestExchange_encodePartition_failsWithoutDataset(t *testing.T) {
	ports := []string{":5551", ":5552", ":5553"}
	distributers := startCluster(t, ports...)
	master := distributers[0]

	defer terminateCluster(t, distributers...)

	ctx := context.WithValue(context.Background(), distributerKey, master)
	ctx = context.WithValue(ctx, allNodesKey, ports)
	ctx = context.WithValue(ctx, masterNodeKey, ports[0])
	ctx = context.WithValue(ctx, thisNodeKey, ports[0])

	partition := Partition(0).(*exchange)
	err := partition.init(ctx)
	require.NoError(t, err)

	err = partition.encodePartition([]int{42})
	require.Error(t, err)
}

func TestExchange_encsMappedByNodes(t *testing.T) {
	ports := []string{":5551", ":5552", ":5553"}
	distributers := startCluster(t, ports...)
	master := distributers[0]

	defer terminateCluster(t, distributers...)

	ctx := context.WithValue(context.Background(), distributerKey, master)
	ctx = context.WithValue(ctx, allNodesKey, ports)
	ctx = context.WithValue(ctx, masterNodeKey, ports[0])
	ctx = context.WithValue(ctx, thisNodeKey, ports[0])

	partition := Partition(0).(*exchange)
	err := partition.init(ctx)
	require.NoError(t, err)

	encKeys := make([]string, 0, len(partition.encsByKey))
	for k := range partition.encsByKey {
		encKeys = append(encKeys, k)
	}
	require.ElementsMatch(t, ports, encKeys)
}
