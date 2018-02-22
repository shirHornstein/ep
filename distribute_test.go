package ep

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var _ = registerGob(&upper{})
var _ = registerGob(&dataRunner{})

func TestDistribute_success(t *testing.T) {
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := mockPeer(t, port1)
	defer dist1.Close()

	port2 := ":5552"
	defer mockPeer(t, port2).Close()

	port3 := ":5553"
	defer mockPeer(t, port3).Close()

	runner := dist1.Distribute(Pipeline(Scatter(), Gather()), port1, port2, port3)

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)

	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, "[hello world foo bar]", fmt.Sprintf("%v", data.At(0)))
}

func TestDistribute_connectionError(t *testing.T) {
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := mockPeer(t, port1)
	defer dist1.Close()

	runner := dist1.Distribute(&upper{}, port1, ":5000")

	data, err := TestRunner(runner, NewDataset())

	require.Error(t, err)
	require.Equal(t, "dial tcp :5000: connect: connection refused", err.Error())
	require.Nil(t, data)
}

// Test that errors are transmitted across the network
func SkipTestDistributeErrorFromPeer(t *testing.T) { //todo: not stable
	// avoid "bind: address already in use" error in future tests
	defer time.Sleep(1 * time.Millisecond)

	port1 := ":5551"
	dist1 := mockPeer(t, port1)
	defer dist1.Close()

	port2 := ":5552"
	defer mockPeer(t, port2).Close()

	mightErrored := &dataRunner{NewDataset(), port2}
	runner := dist1.Distribute(Pipeline(Scatter(), &nodeAddr{}, mightErrored, Gather()), port1, port2)

	data1 := NewDataset(strs{"hello", "world"})
	data2 := NewDataset(strs{"foo", "bar"})
	data, err := TestRunner(runner, data1, data2)

	require.Error(t, err)
	require.Equal(t, "error :5552", err.Error())
	require.Equal(t, 0, data.Width())
}
