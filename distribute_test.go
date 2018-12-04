package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDistributer(t *testing.T) {
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer2 := eptest.NewPeer(t, port2)

	port3 := ":5553"
	peer3 := eptest.NewPeer(t, port3)
	defer func() {
		require.NoError(t, dist1.Close())
		require.NoError(t, peer2.Close())
		require.NoError(t, peer3.Close())
	}()

	runner := dist1.Distribute(ep.Pipeline(ep.Scatter(), ep.Gather()), port1, port2, port3)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)

	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, "[hello world foo bar]", fmt.Sprintf("%v", data.At(0)))
}

func TestDistributer_connectionError(t *testing.T) {
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)
	defer func() { require.NoError(t, dist1.Close()) }()

	runner := dist1.Distribute(&upper{}, port1, ":5000")

	data, err := eptest.Run(runner, ep.NewDataset())

	require.Error(t, err)
	require.Equal(t, "dial tcp :5000: connect: connection refused", err.Error())
	require.Nil(t, data)
}

// Test that errors are transmitted across the network
func TestDistributer_Distribute_errorFromPeer(t *testing.T) {
	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist1.Close())
		require.NoError(t, peer.Close())
	}()

	mightErrored := &dataRunner{Dataset: ep.NewDataset(), ThrowOnData: port2}
	runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, mightErrored)
	runner = dist1.Distribute(runner, port1, port2)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)

	require.Error(t, err)
	require.Equal(t, "error :5552", err.Error())
	require.Equal(t, 0, data.Width())
}

func TestDistributer_Distribute_ignoreCanceledError(t *testing.T) {
	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist1.Close())
		require.NoError(t, peer.Close())
	}()

	var tests = []struct {
		name string
		r    ep.Runner
	}{
		{name: "from peer", r: &dataRunner{ep.NewDataset(str.Data(1)), port2, true}},
		{name: "from master", r: &dataRunner{ep.NewDataset(str.Data(1)), port1, true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, tc.r)
			runner = dist1.Distribute(runner, port1, port2)

			data1 := ep.NewDataset(strs{"hello", "world"})
			data2 := ep.NewDataset(strs{"foo", "bar"})

			_, err := eptest.Run(runner, data1, data2)
			require.NoError(t, err)
		})
	}
}
