package ep_test

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
)

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

func TestStripHashColumn(t *testing.T) {
	ids := strs([]string{"id1", "id2", "id3", "id4"})
	things := strs([]string{"meh", "nya", "shtoot", "foo"})

	data := ep.NewDataset(ids, things)
	newData := ep.StripHashColumn(data)

	require.Equal(t, 4, newData.Len())
	require.Equal(t, 1, newData.Width())
	require.Equal(t, things.Strings(), newData.At(0).Strings())
}

func TestExchange_GetRow(t *testing.T) {
	firstColumn := strs([]string{"one", "two", "forty two"})
	secondColumn := strs([]string{"meh", "shtoot", "", "foo"})

	data := ep.NewDataset(firstColumn, secondColumn)
	row := ep.GetRow(data, 1)

	require.Equal(t, 1, row.Len())
	require.Equal(t, 2, row.Width())
	require.Equal(t, "[[two] [shtoot]]", fmt.Sprintf("%v", row))
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

	runner := ep.Pipeline(ep.Partition(), ep.PassThrough(), ep.Gather())
	runner = dist.Distribute(runner, port1, port2)

	firstColumn := strs{"this", "is", "sparta"}
	secondColumn := strs{"meh", "shtoot", "nya"}

	data := ep.NewDataset(firstColumn, secondColumn)
	res, err := eptest.Run(runner, data)

	require.NoError(t, err)
	require.NotNil(t, res)

	// partition->gather does not ensure the same order of entries
	// first column is discarded
	require.ElementsMatch(t, secondColumn, res.At(0))
}
