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

// project error should cancel all inner runners
func TestProject_error(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &infinityRunner{}
	runner := Project(infinity, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.getIsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProjectWithPipelines_error(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner := &infinityRunner{}
	runner := Project(
		Pipeline(PassThrough(), infinityRunner),
		Pipeline(PassThrough(), &errRunner{err}),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner.getIsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProjectWithOnePipelineDataFirst_error(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner := &infinityRunner{}
	runner := Project(
		Pipeline(infinityRunner, PassThrough()),
		&errRunner{err},
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner.getIsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestNestedProjectWithTwoPipelinesDataLast_error(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner :=
		Pipeline(
			Project(
				Pipeline(PassThrough(), infinityRunner1),
				Pipeline(PassThrough(), infinityRunner2),
			),
			Project(
				Pipeline(PassThrough(), infinityRunner3),
				Pipeline(PassThrough(), &errRunner{err}),
			))
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.getIsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.getIsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.getIsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestNestedProjectWithTwoPipelinesDataFirst_error(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner :=
		Pipeline(
			Project(
				Pipeline(infinityRunner1, PassThrough()),
				Pipeline(infinityRunner2, PassThrough()),
			),
			Project(
				Pipeline(infinityRunner3, PassThrough()),
				Pipeline(&errRunner{err}, PassThrough()),
			))
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.getIsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.getIsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.getIsRunning(), "Infinity go-routine leak")
}

// project error should cancel all inner runners
func TestProjectWithExchange_error(t *testing.T) {
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
	require.Equal(t, false, infinityRunner.getIsRunning(), "Infinity go-routine leak")
}

// Test that Projected runners always returns the same number of rows
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
