package ep

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExampleProject() {
	runner := Project(&upper{}, &question{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[HELLO WORLD] [is hello? is world?]] <nil>
}

func ExampleProject_reversed() {
	runner := Project(&question{}, &upper{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[is hello? is world?] [HELLO WORLD]] <nil>
}

func TestProject_errorInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &infinityRunner{}
	runner := Project(&errRunner{err}, infinity)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.IsRunning(), "Infinity go-routine leak")
}

func TestProject_errorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := Project(infinityRunner1, &errRunner{err}, infinityRunner2)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Project(infinityRunner1, infinityRunner2, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Project(
		Pipeline(infinityRunner1, infinityRunner2),
		Pipeline(infinityRunner3, &errRunner{err}),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	dist := mockPeer(t, port)
	defer dist.Close()

	port2 := ":5552"

	ctx := context.WithValue(context.Background(), distributerKey, dist)
	ctx = context.WithValue(ctx, allNodesKey, []string{port, port2, ":5553"})
	ctx = context.WithValue(ctx, masterNodeKey, port)
	ctx = context.WithValue(ctx, thisNodeKey, port)

	exchange := Scatter().(*exchange)

	runner := Pipeline(
		exchange,
		Project(infinityRunner, &errRunner{err}),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Project(
		Project(infinityRunner3, &errRunner{err}),
		Project(infinityRunner1, infinityRunner2),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Project(
		Project(infinityRunner1, infinityRunner2),
		Project(infinityRunner3, &errRunner{err}),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

// projected runners should return the same number of rows
func TestProject_errorMismatchRows(t *testing.T) {
	runner := Project(&upper{}, &count{})
	data := NewDataset(strs([]string{"hello", "world"}))
	_, err := TestRunner(runner, data)
	require.Error(t, err)
	require.Equal(t, errMismatch, err)
}

func TestProject_Filter(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := Project(q1, &upper{}, q2).(RunnerFilterable)
	runner.Filter([]bool{false, true, true})
	data := NewDataset(strs([]string{"hello", "world"}))

	data, err := TestRunner(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.True(t, q2.called)
	require.Equal(t, "[[] [HELLO WORLD] [is hello? is world?]]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_all(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := Project(q1, &upper{}, q2).(RunnerFilterable)
	runner.Filter([]bool{false, false, false})
	data := NewDataset(strs([]string{"hello", "world"}))

	data, err := TestRunner(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[[] [] []]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalPartial(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	q3 := &question{}
	q4 := &question{}
	internalProject := Project(q2, &upper{}, q3).(RunnerFilterable)
	runner := Project(q1, internalProject, q4).(RunnerFilterable)
	runner.Filter([]bool{false, false, true, true, false})
	data := NewDataset(strs([]string{"hello", "world"}))

	data, err := TestRunner(runner, data)
	require.NoError(t, err)

	require.Equal(t, 5, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.True(t, q3.called)
	require.False(t, q4.called)
	require.Equal(t, "[[] [] [HELLO WORLD] [is hello? is world?] []]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalAll(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	internalProject := Project(q1, &upper{}).(RunnerFilterable)
	runner := Project(internalProject, q2).(RunnerFilterable)
	runner.Filter([]bool{false, false, false})
	data := NewDataset(strs([]string{"hello", "world"}))

	data, err := TestRunner(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[[] [] []]", fmt.Sprintf("%+v", data.Strings()))
}

type count struct{}

func (*count) Returns() []Type { return []Type{str} }
func (*count) Run(_ context.Context, inp, out chan Dataset) error {
	c := 0
	for data := range inp {
		c += data.Len()
	}

	out <- NewDataset(strs{fmt.Sprintf("%d", c)})
	return nil
}
