package ep_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
)

var _ = ep.Runners.Register("datasetSize", &datasetSize{})

// Example of Scatter with just 2 nodes. The datasets are scattered in
// round-robin to the two nodes such that each node receives half of the
// datasets. Thus the output in the local node just returns half of the output.
// Pipelining into a Gather runner would recollected the scattered outputs
func ExampleScatter() {
	ln1, _ := net.Listen("tcp", ":5551")
	dist1 := ep.NewDistributer(":5551", ln1)
	defer dist1.Close()

	ln2, _ := net.Listen("tcp", ":5552")
	dist2 := ep.NewDistributer(":5552", ln2)
	defer dist2.Close()

	runner := dist1.Distribute(ep.Scatter(), ":5551", ":5552")

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)
	fmt.Println(data.Strings(), err) // no gather - only one batch should return

	// Output:
	// [[foo bar]] <nil>
}

func TestExchange_dialingError(t *testing.T) {
	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer2 := eptest.NewDialingErrorPeer(t, port2)

	port3 := ":5553"
	peer3 := eptest.NewPeer(t, port3)
	defer func() {
		require.NoError(t, dist1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	runner := dist1.Distribute(ep.Scatter(), port1, port2, port3)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)

	require.Error(t, err)

	possibleErrors := []string{
		// when interacting with peers after their failure
		"write tcp",
		"read tcp",

		"bad connection from port :5552",        // reported by 5552, when dialing to :5553
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
	dist := eptest.NewPeer(t, port)
	defer func() {
		require.NoError(t, dist.Close())
	}()

	runner := dist.Distribute(ep.Scatter(), port)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)
	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, 4, data.Len())
}

func TestScatter_and_Gather(t *testing.T) {
	port1 := ":5551"
	dist := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, ep.Gather())
	runner = dist.Distribute(runner, port1, port2)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)

	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "[[hello world foo bar] [:5552 :5552 :5551 :5551]]", fmt.Sprintf("%v", data))
}

func TestPartition_AndGather(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	maxPort := 7000
	minPort := 6000
	randomPort := rand.Intn(maxPort-minPort) + minPort

	port1 := fmt.Sprintf(":%d", randomPort)
	dist := eptest.NewPeer(t, port1)

	port2 := fmt.Sprintf(":%d", randomPort+1)
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	runner := ep.Pipeline(ep.Partition(0), ep.PassThrough(), ep.Gather())
	runner = dist.Distribute(runner, port1, port2)

	firstColumn := strs{"this", "is", "sparta"}
	secondColumn := strs{"meh", "shtoot", "nya"}

	data := ep.NewDataset(firstColumn, secondColumn)
	res, err := eptest.Run(runner, data)

	require.NoError(t, err)
	require.NotNil(t, res)

	// partition->gather does not ensure the same order of entries
	require.ElementsMatch(t, firstColumn, res.At(0))
	require.ElementsMatch(t, secondColumn, res.At(1))
}

func TestPartition_UsesProvidedColumn(t *testing.T) {
	port1 := fmt.Sprintf(":%d", 5551)
	dist := eptest.NewPeer(t, port1)

	port2 := fmt.Sprintf(":%d", 5552)
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	// to the exact opposite
	// deliberately opposite values: column switch has to change to output
	firstColumn := strs{"one", "two"}
	secondColumn := strs{"two", "one"}

	data := ep.NewDataset(firstColumn, secondColumn)

	runner := ep.Pipeline(ep.Partition(0), &nodeAddr{}, ep.Gather())
	runner = dist.Distribute(runner, port1, port2)
	firstRes, err := eptest.Run(runner, data)

	require.NoError(t, err)
	require.NotNil(t, firstRes)

	runner = ep.Pipeline(ep.Partition(1), &nodeAddr{}, ep.Gather())
	runner = dist.Distribute(runner, port1, port2)
	secondRes, err := eptest.Run(runner, data)

	require.NoError(t, err)
	require.NotNil(t, secondRes)

	/*
		Expected output similar to:
		[[one two] [two one] [:5552 :5551]]
		[[two one] [one two] [:5552 :5551]]
	*/

	firstResAt0 := firstRes.At(0)
	firstResAt1 := firstRes.At(1)
	secondResAt1 := secondRes.At(1)
	secondResAt0 := secondRes.At(0)
	if reflect.DeepEqual(firstRes.At(2), secondRes.At(2)) {
		// node addresses are the same - data should be different
		require.Equalf(t, firstResAt0, secondResAt1, "%s != %s", firstResAt0, secondResAt1)
		require.Equalf(t, firstResAt1, secondResAt0, "%s != %s", firstResAt1, secondResAt0)
	} else {
		// node addresses are different - data should be the same
		require.Equalf(t, firstResAt0, secondResAt0, "%s != %s", firstResAt0, secondResAt0)
		require.Equalf(t, firstResAt1, secondResAt1, "%s != %s", firstResAt1, secondResAt1)
	}
}

func TestPartition_SendsCompleteDatasets(t *testing.T) {
	port1 := fmt.Sprintf(":%d", 5551)
	dist := eptest.NewPeer(t, port1)

	port2 := fmt.Sprintf(":%d", 5552)
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	firstColumn := strs{"foo", "bar", "meh", "nya", "shtoot", "a", "few", "more", "things"}
	secondColumn := strs{"f", "s", "f", "f", "s", "f", "f", "f", "s"}

	data := ep.NewDataset(firstColumn, secondColumn)
	runner := ep.Pipeline(ep.Partition(1), &datasetSize{}, ep.Gather())
	runner = dist.Distribute(runner, port1, port2)

	res, err := eptest.Run(runner, data)
	require.NoError(t, err)

	// there are 6 "f" and 3 "s" in second column which is used for partitioning
	expected := []string{"6", "3"}
	sizes := res.At(0)

	require.Equal(t, 2, sizes.Len())
	require.ElementsMatch(t, expected, sizes.Strings())
}

type datasetSize struct{}

func (*datasetSize) Returns() []ep.Type { return []ep.Type{str} }
func (*datasetSize) Run(_ context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		n := fmt.Sprintf("%v", data.Len())
		out <- ep.NewDataset(strs{n})
	}
	return nil
}
