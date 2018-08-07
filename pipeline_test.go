package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExamplePipeline() {
	runner := ep.Pipeline(&upper{}, &question{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[is HELLO? is WORLD?]] <nil>

}

func ExamplePipeline_reverse() {
	runner := ep.Pipeline(&question{}, &upper{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[IS HELLO? IS WORLD?]] <nil>
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := ep.Pipeline(NewErrRunner(err), infinityRunner1, infinityRunner2)
	data := ep.NewDataset(str.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := ep.Pipeline(infinityRunner1, NewErrRunner(err), infinityRunner2)
	data := ep.NewDataset(str.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInThirdRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := ep.Pipeline(infinityRunner1, infinityRunner2, NewErrRunner(err))
	data := ep.NewDataset(str.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInNestedPipeline(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	runner := ep.Pipeline(
		ep.Pipeline(infinityRunner1, NewErrRunner(err)),
		ep.Pipeline(infinityRunner2, infinityRunner3),
	)
	data := ep.NewDataset(str.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
// project error should cancel all inner runners
func TestPipeline_errNestedPipelineWithProject(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	infinityRunner3 := &infinityRunner{}
	infinityRunner4 := &infinityRunner{}
	infinityRunner5 := &infinityRunner{}
	infinityRunner6 := &infinityRunner{}
	infinityRunner7 := &infinityRunner{}
	runner :=
		ep.Pipeline(
			ep.Project(
				ep.Pipeline(infinityRunner1, infinityRunner4),
				ep.Pipeline(infinityRunner2, infinityRunner5),
			),
			ep.Project(
				ep.Pipeline(infinityRunner3, infinityRunner6),
				ep.Pipeline(infinityRunner7, NewErrRunner(err)),
			))
	data := ep.NewDataset(str.Data(1))
	data, err = eptest.Run(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner2.IsRunning(), "Infinity go-routine leak")
	require.Equal(t, false, infinityRunner3.IsRunning(), "Infinity go-routine leak")
}

func TestPipeline_Returns_wildcard(t *testing.T) {
	runner := ep.Project(&upper{}, &question{})
	runner = ep.Pipeline(runner, ep.PassThrough())
	types := runner.Returns()
	require.Equal(t, 2, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, str.Name(), types[1].Name())

	runner = ep.Project(&upper{}, &question{})
	runner = ep.Pipeline(runner, ep.Pick(1))
	types = runner.Returns()
	require.Equal(t, 1, len(types))
}

func TestPipeline_Returns_wildcardIdx(t *testing.T) {
	runner := ep.Project(&upper{}, &question{}, &question{})
	runner = ep.Pipeline(runner, ep.Pick(1, 2))
	types := runner.Returns()
	require.Equal(t, 2, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, str.Name(), types[1].Name())
	require.Equal(t, "question", ep.GetAlias(types[0]))
	require.Equal(t, "question", ep.GetAlias(types[1]))
}

func TestPipeline_Returns_wildcardMinusTail(t *testing.T) {
	runner := ep.Project(&upper{}, &question{}, &upper{})
	runner = ep.Pipeline(runner, &tailCutter{2})

	types := runner.Returns()
	require.Equal(t, 1, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, "upper", ep.GetAlias(types[0]))
}

type tailCutter struct {
	CutFromTail int
}

func (r *tailCutter) Returns() []ep.Type { return []ep.Type{ep.WildcardMinusTail(r.CutFromTail)} }
func (*tailCutter) Run(_ context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		out <- data
	}
	return nil
}
