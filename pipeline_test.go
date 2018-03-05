package ep

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExamplePipeline() {
	runner := Pipeline(&upper{}, &question{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data, err)

	// Output: [[is HELLO? is WORLD?]] <nil>

}

func ExamplePipeline_reverse() {
	runner := Pipeline(&question{}, &upper{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data, err)

	// Output: [[IS HELLO? IS WORLD?]] <nil>
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipeline_errInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &infinityRunner{}
	infinityRunner2 := &infinityRunner{}
	runner := Pipeline(&errRunner{err}, infinityRunner1, infinityRunner2)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Pipeline(infinityRunner1, &errRunner{err}, infinityRunner2)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Pipeline(infinityRunner1, infinityRunner2, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
	runner := Pipeline(
		Pipeline(infinityRunner1, &errRunner{err}),
		Pipeline(infinityRunner2, infinityRunner3),
	)
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

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
		Pipeline(
			Project(
				Pipeline(infinityRunner1, infinityRunner4),
				Pipeline(infinityRunner2, infinityRunner5),
			),
			Project(
				Pipeline(infinityRunner3, infinityRunner6),
				Pipeline(infinityRunner7, &errRunner{err}),
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

func TestPipeline_creation_empty_panic(t *testing.T) {
	require.Panics(t, func() { Pipeline() })
}

func TestPipeline_creation_single_runner(t *testing.T) {
	runner := Pipeline(&upper{})
	_, isPipe := runner.(pipeline)
	require.False(t, isPipe)

	passThrough := Pipeline(PassThrough())
	_, isPipe = passThrough.(pipeline)
	require.False(t, isPipe)

	projectWithPipeline := Pipeline(Project(Pipeline(&question{}, &question{}), &upper{}))
	_, isPipe = projectWithPipeline.(pipeline)
	require.False(t, isPipe)

	nestedPipeline := Pipeline(Pipeline(&question{}, &question{}))
	p, isPipe := nestedPipeline.(pipeline)
	require.True(t, isPipe)
	require.Equal(t, 2, len(p))
	_, isPipe = p[0].(pipeline)
	require.False(t, isPipe)
}

func TestPipeline_creation_flat(t *testing.T) {
	runner := Pipeline(&upper{}, &question{})
	runner = Pipeline(runner, Pipeline(&question{}, Pipeline(&question{}, &question{})))

	p, isPipe := runner.(pipeline)
	require.True(t, isPipe)
	require.Equal(t, 5, len(p))
}

func TestPipeline_creation_flat_skip_passthrogh(t *testing.T) {
	runner := Pipeline(&upper{}, PassThrough())
	runner = Pipeline(runner, Pipeline(&question{}, Pipeline(PassThrough(), &question{})))

	p, isPipe := runner.(pipeline)
	require.True(t, isPipe)
	// 5 runners without 2 skipped passThrough
	require.Equal(t, 3, len(p))
}

func TestPipeline_creation_dont_flat_project(t *testing.T) {
	runner := Project(&upper{}, Pipeline(&question{}, &question{}), Pipeline(&question{}, &question{}))
	runner = Pipeline(runner, &upper{})

	p, isPipe := runner.(pipeline)
	require.True(t, isPipe)
	require.Equal(t, 2, len(p))
}

func TestPipeline_creation_single_runner_after_flat(t *testing.T) {
	runner := Pipeline(PassThrough(), &upper{}, PassThrough())
	_, isPipe := runner.(pipeline)
	require.False(t, isPipe)

	nestedPipeline := Pipeline(PassThrough(), Pipeline(&upper{}, PassThrough()), PassThrough())
	_, isPipe = nestedPipeline.(pipeline)
	require.False(t, isPipe)

	onlyPassThrough := Pipeline(PassThrough(), PassThrough())
	_, isPipe = onlyPassThrough.(pipeline)
	require.False(t, isPipe)
	_, isPassThrough := onlyPassThrough.(*passthrough)
	require.True(t, isPassThrough)

	nestedPipelineWithOnlyPassThrough := Pipeline(
		PassThrough(),
		Pipeline(PassThrough(), PassThrough()),
		PassThrough(),
		Pipeline(PassThrough(), Pipeline(PassThrough(), PassThrough())),
	)
	_, isPipe = nestedPipelineWithOnlyPassThrough.(pipeline)
	require.False(t, isPipe)
	_, isPassThrough = onlyPassThrough.(*passthrough)
	require.True(t, isPassThrough)
}

func TestPipeline_Returns_wildcard(t *testing.T) {
	runner := Project(&upper{}, &question{})
	runner = Pipeline(runner, PassThrough())
	types := runner.Returns()
	require.Equal(t, 2, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, str.Name(), types[1].Name())

	runner = Project(&upper{}, &question{})
	runner = Pipeline(runner, Pick(1))
	types = runner.Returns()
	require.Equal(t, 1, len(types))
}

func TestPipeline_Returns_wildcardIdx(t *testing.T) {
	runner := Project(&upper{}, &question{})
	runner = Pipeline(runner, Pick(1))
	types := runner.Returns()
	require.Equal(t, 1, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, "question", GetAlias(types[0]))
}

func TestPipeline_Returns_wildcardMinusTail(t *testing.T) {
	runner := Project(&upper{}, &question{}, &upper{})
	runner = Pipeline(runner, &tailCutter{2})

	types := runner.Returns()
	require.Equal(t, 1, len(types))
	require.Equal(t, str.Name(), types[0].Name())
	require.Equal(t, "upper", GetAlias(types[0]))
}

// Measures the number of datasets (ops) per second going through a pipeline
// composed of 3 pass-throughs. At the time of writing, it was evident that
// performance is not impacted by the size of the datasets (sensible, given
// the implementation details).
func BenchmarkPipeline(b *testing.B) {
	data := NewDataset(strs([]string{"hello", "world", "foo", "bar"}))
	inp := make(chan Dataset)
	out := make(chan Dataset)
	defer close(inp)
	defer close(out)

	r := pipeline{PassThrough(), PassThrough(), PassThrough()}
	go r.Run(context.Background(), inp, out)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// run a single dataset through the pipeline
		inp <- data
		<-out
	}
}

type tailCutter struct {
	CutFromTail int
}

func (r *tailCutter) Returns() []Type { return []Type{WildcardMinusTail(r.CutFromTail)} }
func (*tailCutter) Run(_ context.Context, inp, out chan Dataset) error {
	for data := range inp {
		out <- data
	}
	return nil
}
