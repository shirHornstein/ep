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
	// [(is HELLO?) (is WORLD?)] <nil>

}

func ExamplePipeline_reverse() {
	runner := ep.Pipeline(&question{}, &upper{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [(IS HELLO?) (IS WORLD?)] <nil>
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	runner := ep.Pipeline(NewErrRunner(err), infinityRunner1, infinityRunner2)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	runner := ep.Pipeline(infinityRunner1, NewErrRunner(err), infinityRunner2)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInThirdRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	runner := ep.Pipeline(infinityRunner1, infinityRunner2, NewErrRunner(err))
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInNestedPipeline(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	infinityRunner3 := &waitForCancel{}
	runner := ep.Pipeline(
		ep.Pipeline(infinityRunner1, NewErrRunner(err)),
		ep.Pipeline(infinityRunner2, infinityRunner3),
	)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity go-routine leak")
	require.False(t, infinityRunner3.IsRunning(), "Infinity go-routine leak")
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
// project error should cancel all inner runners
func TestPipeline_errNestedPipelineWithProject(t *testing.T) {
	pipeLength := 8
	err := fmt.Errorf("something bad happened")

	for errIdx := 0; errIdx < pipeLength; errIdx++ {
		t.Run(fmt.Sprintf("error in runner %d", errIdx), func(t *testing.T) {
			runners := make([]ep.Runner, 8)
			for i := range runners {
				if i == errIdx {
					runners[i] = NewErrRunner(err)
				} else {
					runners[i] = &waitForCancel{}
				}
			}
			runner := ep.Pipeline(
				ep.Project(
					ep.Pipeline(runners[0], runners[1]),
					ep.Pipeline(runners[2], runners[3]),
				),
				ep.Project(
					ep.Pipeline(runners[4], runners[5]),
					ep.Pipeline(runners[6], runners[7]),
				),
			)

			data := ep.NewDataset(str.Data(1))
			_, resErr := eptest.Run(runner, data)

			require.Error(t, resErr)
			require.Equal(t, err.Error(), resErr.Error())
			for i, r := range runners {
				if i != errIdx {
					require.False(t, r.(*waitForCancel).IsRunning(), "Infinity go-routine leak")
				}
			}
		})
	}
}

func TestPipeline_ignoreCanceledError(t *testing.T) {
	runner := ep.Pipeline(&dataRunner{Dataset: ep.NewDataset(str.Data(1)), ThrowOnData: "cancel", ThrowCanceled: true}, &count{})

	data1 := ep.NewDataset(strs{"not cancel"})
	data2 := ep.NewDataset(strs{"cancel"})
	res, err := eptest.Run(runner, data1, data2)

	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Width())
	require.Equal(t, 1, res.Len())
	require.Equal(t, []string{"(1)"}, res.Strings())
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

func TestPipeline_Args_runnerArgs(t *testing.T) {
	// two runners are required to create an instance of pipeline
	runner := ep.Pipeline(&runnerWithArgs{}, &runnerWithoutArgs{})

	runnerArgs, ok := runner.(ep.RunnerArgs)
	require.True(t, ok)

	args := runnerArgs.Args()
	require.Equal(t, []ep.Type{ep.Any}, args)
}

func TestPipeline_Args_noArgs(t *testing.T) {
	// two runners are required to create an instance of pipeline
	runner := ep.Pipeline(&runnerWithoutArgs{}, &runnerWithArgs{})

	runnerArgs, ok := runner.(ep.RunnerArgs)
	require.True(t, ok)

	args := runnerArgs.Args()
	require.Equal(t, []ep.Type{ep.Wildcard}, args)
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

type runnerWithArgs struct{ ep.Runner }

func (r *runnerWithArgs) Args() []ep.Type { return []ep.Type{ep.Any} }

type runnerWithoutArgs struct{ ep.Runner }
