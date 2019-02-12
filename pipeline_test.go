package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"strconv"
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

func TestPipeline_ApproxSize(t *testing.T) {
	t.Run("known size", func(t *testing.T) {
		r := ep.Pipeline(&upper{}, &runnerWithSize{size: 42})
		sizer, ok := r.(ep.ApproxSizer)
		require.True(t, ok)
		require.Equal(t, 42, sizer.ApproxSize())
	})

	t.Run("unknown size", func(t *testing.T) {
		r := ep.Pipeline(&upper{}, &runnerWithSize{size: 42}, &question{})
		sizer, ok := r.(ep.ApproxSizer)
		require.True(t, ok)
		require.Equal(t, ep.SizeUnknown, sizer.ApproxSize())
	})
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

// errors handling
func TestPipeline_errorPropagation(t *testing.T) {
	pipeLength := 4
	err := fmt.Errorf("something bad happened")

	for errIdx := 0; errIdx < pipeLength; errIdx++ {
		t.Run(fmt.Sprintf("error in runner %d", errIdx), func(t *testing.T) {
			runners := make([]ep.Runner, pipeLength)
			for i := range runners {
				if i == errIdx {
					runners[i] = eptest.NewErrRunner(err)
				} else {
					runners[i] = &waitForCancel{}
				}
			}
			runner := ep.Pipeline(runners...)

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

func TestPipeline_multipleErrorsPropagation(t *testing.T) {
	pipeLength := 10
	err := fmt.Errorf("something bad happened")

	for errIdx := 0; errIdx < pipeLength; errIdx++ {
		t.Run(strconv.Itoa(errIdx), func(t *testing.T) {
			runners := make([]ep.Runner, pipeLength)
			for i := range runners {
				if i == errIdx || i == (errIdx+2)%pipeLength {
					runners[i] = eptest.NewErrRunner(err)
				} else {
					runners[i] = ep.PassThrough()
				}
			}
			runner := ep.Pipeline(
				runners[0],
				// project error should cancel all inner runners
				ep.Project(
					ep.Pipeline(runners[1], runners[2], runners[3], runners[4]),
					runners[5],
					ep.Pipeline(runners[6], runners[7], runners[8]),
				),
				runners[9],
				ep.Project(
					ep.Pipeline(runners[5], runners[6]),
					ep.Pipeline(runners[7], runners[8]),
				),
				runners[9],
			)

			runVerifyError(t, runner, err)
		})
	}
}

func TestPipeline_errorPropagationWithProject(t *testing.T) {
	pipeLength := 4
	err := fmt.Errorf("something bad happened")

	for errIdx := 0; errIdx < pipeLength; errIdx++ {
		t.Run(fmt.Sprintf("error in runner %d", errIdx), func(t *testing.T) {
			runners := make([]ep.Runner, pipeLength)
			for i := range runners {
				if i == errIdx {
					runners[i] = eptest.NewErrRunner(err)
				} else {
					runners[i] = ep.PassThrough()
				}
			}

			t.Run("project at the beginning", func(t *testing.T) {
				runner := ep.Pipeline(
					ep.Project(
						ep.Pipeline(runners[1], runners[2]),
						runners[3],
					),
					runners[0],
				)
				runVerifyError(t, runner, err)
			})

			t.Run("project at the end", func(t *testing.T) {
				runner := ep.Pipeline(
					runners[0],
					ep.Project(
						ep.Pipeline(runners[1], runners[2]),
						runners[3],
					),
				)
				runVerifyError(t, runner, err)
			})
		})
	}
}

func runVerifyError(t *testing.T, runner ep.Runner, expected error) {
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	ctx, cancel := context.WithCancel(context.Background())
	var resErr error

	go ep.Run(ctx, runner, inp, out, cancel, &resErr)
	inp <- ep.NewDataset(str.Data(1))
	for range out {
	}

	require.Error(t, resErr)
	require.Equal(t, expected.Error(), resErr.Error())
	require.NotPanics(t, func() { close(inp) })
}
