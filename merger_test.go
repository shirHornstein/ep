package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestScatter_and_GatherMerger(t *testing.T) {
	port1 := ":5551"
	dist := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, ep.Merge([]ep.SortingCol{{0, false}}))
	runner = dist.Distribute(runner, port1, port2)

	data1 := ep.NewDataset(strs{"hello", "world", "what"})
	data2 := ep.NewDataset(strs{"bar", "foo", "j", "yes"})
	data3 := ep.NewDataset(strs{"z"})
	data, err := eptest.Run(runner, data1, data2, data3)

	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, 2, data.Width())
	require.Equal(t, 8, data.Len())
	require.Equal(t, "[[bar foo hello j world what yes z] [:5551 :5551 :5552 :5551 :5552 :5552 :5551 :5552]]", fmt.Sprintf("%v", data))
}
