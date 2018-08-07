package ep_test

import (
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
	// [(HELLO,is hello?) (WORLD,is world?)] <nil>
}

func ExampleProject_reversed() {
	runner := ep.Project(&question{}, &upper{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [(is hello?,HELLO) (is world?,WORLD)] <nil>
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
	port := ":5551"
	dist := eptest.NewPeer(t, port)

	port2 := ":5559"
	peer2 := eptest.NewPeer(t, port2)
	defer func() {
		require.NoError(t, dist.Close())
		require.NoError(t, peer2.Close())
	}()

	infinityRunner := &infinityRunner{}
	mightErrored := &dataRunner{ep.NewDataset(ep.Null.Data(1)), port2}
	runner := ep.Pipeline(
		infinityRunner,
		ep.Scatter(),
		ep.Project(ep.Broadcast(), ep.Pipeline(&nodeAddr{}, mightErrored)),
		ep.Gather(),
	)
	runner = dist.Distribute(runner, port, port2)

	data := ep.NewDataset(ep.Null.Data(1))
	data, err := eptest.Run(runner, data, data, data, data)

	require.Error(t, err)
	require.Equal(t, "error "+port2, err.Error())
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

func TestProject_Filter(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := ep.Project(q1, &upper{}, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, true, true})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.True(t, q2.called)
	require.Equal(t, "[(HELLO,is hello?) (WORLD,is world?)]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_all(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := ep.Project(q1, &upper{}, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_allWithNested(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	internalProject := ep.Project(q1, &upper{}).(ep.FilterRunner)
	runner := ep.Project(internalProject, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalPartial(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	q3 := &question{}
	q4 := &question{}
	internalProject := ep.Project(q2, &upper{}, q3).(ep.FilterRunner)
	runner := ep.Project(q1, internalProject, q4).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true, true, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 5, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.True(t, q3.called)
	require.False(t, q4.called)
	require.Equal(t, "[(HELLO,is hello?) (WORLD,is world?)]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalAll(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	internalProject := ep.Project(q1, &upper{}).(ep.FilterRunner)
	runner := ep.Project(internalProject, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.True(t, q2.called)
	require.Equal(t, "[(is hello?) (is world?)]", fmt.Sprintf("%+v", data.Strings()))
}
