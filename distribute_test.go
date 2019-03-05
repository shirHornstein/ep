package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDistributer(t *testing.T) {
	runner := ep.Pipeline(ep.Scatter(), ep.Gather())

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})

	data, err := eptest.RunDist(t, 3, runner, data1, data2)

	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, "[hello world foo bar]", fmt.Sprintf("%v", data.At(0)))
}

func TestDistributer_connectionError(t *testing.T) {
	port := ":5551"
	dist := eptest.NewPeer(t, port)
	defer eptest.ClosePeer(t, dist)

	runner := dist.Distribute(&upper{}, port, ":5000")

	data, err := eptest.Run(runner, ep.NewDataset())

	require.Error(t, err)
	require.Equal(t, "dial tcp :5000: connect: connection refused", err.Error())
	require.Nil(t, data)
}

// Test that errors are transmitted across the network
func TestDistributer_Distribute_errorFromPeer(t *testing.T) {
	mightErrored := &dataRunner{ThrowOnData: ":5552"}
	runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, mightErrored)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.RunDist(t, 4, runner, data1, data2)

	require.Error(t, err)
	require.Equal(t, "error :5552", err.Error())
	require.Equal(t, 2, data.Width())
}

func TestDistributer_Distribute_ignoreErrIgnorable(t *testing.T) {
	var tests = []struct {
		name string
		r    ep.Runner
	}{
		{name: "from peer", r: &dataRunner{ThrowOnData: ":5552", ThrowIgnorable: true}},
		{name: "from master", r: &dataRunner{ThrowOnData: ":5551", ThrowIgnorable: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, tc.r)

			data1 := ep.NewDataset(strs{"hello", "world"})
			data2 := ep.NewDataset(strs{"foo", "bar"})

			_, err := eptest.RunDist(t, 4, runner, data1, data2)
			require.NoError(t, err)
		})
	}
}
