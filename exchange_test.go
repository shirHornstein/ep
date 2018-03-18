package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"net"
	"strings"
	"testing"
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
