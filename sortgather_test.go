package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSortGather(t *testing.T) {
	port1 := ":5551"
	dist := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	runSortGather := func(t *testing.T, sortingCols []ep.SortingCol, expected string, datasets ...ep.Dataset) {
		runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, &localSort{Conditions: sortingCols}, ep.SortGather(sortingCols))
		runner = dist.Distribute(runner, port1, port2)
		data, err := eptest.Run(runner, datasets...)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Equal(t, 2, data.Width())
		require.Equal(t, 9, data.Len())
		require.Equal(t, expected, fmt.Sprintf("%v", data))
	}

	t.Run("no data", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: false}}

		runner := ep.Pipeline(ep.Scatter(), &nodeAddr{}, ep.SortGather(sortingCols))
		runner = dist.Distribute(runner, port1, port2)
		data, err := eptest.Run(runner)
		require.NoError(t, err)
		require.Nil(t, data)
	})

	t.Run("asc", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: false}}
		data1 := ep.NewDataset(strs{"hello", "world", "what"})
		data2 := ep.NewDataset(strs{"bar", "foo", "j", "yes"})
		data3 := ep.NewDataset(strs{"yes", "z"})
		expected := "[[bar foo hello j what world yes yes z] [:5552 :5552 :5552 :5551 :5551 :5552 :5551 :5552 :5551]]"

		runSortGather(t, sortingCols, expected, data1, data2, data3)
	})

	t.Run("desc", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: true}}
		data1 := ep.NewDataset(strs{"z", "yes"})
		data2 := ep.NewDataset(strs{"yes", "j", "foo", "bar"})
		data3 := ep.NewDataset(strs{"what", "world", "hello"})
		expected := "[[z yes yes world what j hello foo bar] [:5552 :5552 :5551 :5552 :5552 :5552 :5551 :5551 :5551]]"

		runSortGather(t, sortingCols, expected, data1, data2, data3)
	})

	t.Run("multiple columns", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: false}, {Index: 1, Desc: true}}
		data1 := ep.NewDataset(strs{"foo", "world", "what"})
		data2 := ep.NewDataset(strs{"bar", "foo", "j", "yes"})
		data3 := ep.NewDataset(strs{"yes", "z"})
		expected := "[[bar foo foo j what world yes yes z] [:5552 :5552 :5552 :5551 :5551 :5552 :5552 :5551 :5551]]"

		runSortGather(t, sortingCols, expected, data1, data2, data3)
	})
}

func TestSortGather_error(t *testing.T) {
	port1 := ":5551"
	dist := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer.Close())
	}()

	runSortGather := func(t *testing.T, sortingCols []ep.SortingCol, datasets ...ep.Dataset) {
		var runner ep.Runner = &dataRunner{Dataset: ep.NewDataset(strs{"str"}), ThrowOnData: "failed"}
		runner = ep.Pipeline(runner, ep.Scatter(), &nodeAddr{}, ep.SortGather(sortingCols))
		runner = dist.Distribute(runner, port1, port2)
		_, err := eptest.Run(runner, datasets...)
		require.Error(t, err)
		require.Equal(t, "error failed", err.Error())
	}

	t.Run("error from distributer node", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: true}}
		data2 := ep.NewDataset(strs{"failed"})
		data1 := ep.NewDataset(strs{"z", "yes"})

		runSortGather(t, sortingCols, data1, data2)
	})

	t.Run("error from peer", func(t *testing.T) {
		sortingCols := []ep.SortingCol{{Index: 0, Desc: true}}
		data1 := ep.NewDataset(strs{"z", "yes"})
		data2 := ep.NewDataset(strs{"failed"})
		data3 := ep.NewDataset(strs{"foo"})

		runSortGather(t, sortingCols, data1, data2, data3)
	})
}
