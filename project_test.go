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
	fmt.Println(data, err)

	// Output:
	// [[HELLO WORLD] [is hello? is world?]] <nil>
}

func ExampleProject_reversed() {
	runner := Project(&question{}, &upper{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data, err)

	// Output:
	// [[is hello? is world?] [HELLO WORLD]] <nil>
}

func TestProject_cancelUponErrorInFirstRunner(t *testing.T) {
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

func TestProject_cancelUponErrorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := Project(infinityRunner1, &errRunner{err}, infinityRunner2)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

func TestProject_cancelUponErrorInThirdRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := Project(infinityRunner1, infinityRunner2, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProject_withPipelines_error(t *testing.T) {
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
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity go-routine leak 3")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak 2")
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak 1")
}

// project error should cancel all inner runners
func TestProject_NestedWithErrorInTheFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner :=
		Project(
			Project(
				infinityRunner3,
				&errRunner{err},
			),
			Project(
				infinityRunner1,
				infinityRunner2,
			))
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProject_NestedWithErrorInTheSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner :=
		Project(
			Project(
				infinityRunner1,
				infinityRunner2,
			),
			Project(
				infinityRunner3,
				&errRunner{err},
			))
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProject_withExchange_error(t *testing.T) {
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

	runner :=
		Pipeline(
			exchange,
			Project(
				Pipeline(PassThrough(), infinityRunner),
				Pipeline(PassThrough(), &errRunner{err}),
			))
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner.IsRunning(), "Infinity go-routine leak")
}

// Test that projected runners always returns the same number of rows
func TestProject_mismatchErr(t *testing.T) {
	runner := Project(&upper{}, &count{})
	data := NewDataset(strs([]string{"hello", "world"}))
	_, err := TestRunner(runner, data)
	require.Error(t, err)
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
