package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExampleProject() {
	runner := ep.Project(&upper{}, &question{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[HELLO WORLD] [is hello? is world?]] <nil>
}

func ExampleProject_reversed() {
	runner := ep.Project(&question{}, &upper{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[is hello? is world?] [HELLO WORLD]] <nil>
}

func TestProject_errorInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &infinityRunner{}
	runner := ep.Project(NewErrRunner(err), infinity)
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.IsRunning(), "Infinity go-routine leak")
}

func TestProject_errorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := ep.Project(infinityRunner1, NewErrRunner(err), infinityRunner2)
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
}

func TestProject_errorInThirdRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := ep.Project(infinityRunner1, infinityRunner2, NewErrRunner(err))
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
}

func TestProject_errorInPipeline(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner := ep.Project(
		ep.Pipeline(infinityRunner1, infinityRunner2),
		ep.Pipeline(infinityRunner3, NewErrRunner(err)),
	)
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

func TestProject_errorWithExchange(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner := &infinityRunner{}

	port := ":5551"
	dist := eptest.NewPeer(t, port)
	defer dist.Close()

	port2 := ":5552"
	peer2 := eptest.NewPeer(t, port2)
	defer peer2.Close()

	runner := ep.Pipeline(
		infinityRunner,
		ep.Project(ep.Scatter(), NewErrRunner(err)),
	)
	runner = dist.Distribute(runner, port, port2)

	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data, data, data, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner.IsRunning(), "Infinity go-routine leak")
}

func TestProject_nested_errorInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner := ep.Project(
		ep.Project(infinityRunner3, NewErrRunner(err)),
		ep.Project(infinityRunner1, infinityRunner2),
	)
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

func TestProject_nested_errorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner := ep.Project(
		ep.Project(infinityRunner1, infinityRunner2),
		ep.Project(infinityRunner3, NewErrRunner(err)),
	)
	data := ep.NewDataset(ep.Null.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

// projected runners should return the same number of rows
func TestProject_errorMismatchRows(t *testing.T) {
	runner := ep.Project(&upper{}, &count{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	_, err := eptest.Run(runner, data)
	require.Error(t, err)
	require.Equal(t, "mismatched number of rows", err.Error())
}

type count struct{}

func (*count) Returns() []ep.Type { return []ep.Type{str} }
func (*count) Run(_ context.Context, inp, out chan ep.Dataset) error {
	c := 0
	for data := range inp {
		c += data.Len()
	}

	out <- ep.NewDataset(strs{fmt.Sprintf("%d", c)})
	return nil
}
