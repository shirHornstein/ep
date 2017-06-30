package ep

import (
    "fmt"
    "testing"
    "github.com/stretchr/testify/require"
)

func ExamplePipeline() {
    runner := Pipeline(&Upper{}, &Question{})
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err := testRun(runner, data)
    fmt.Println(data, err)

    // Output: [[is HELLO? is WORLD?]] <nil>

}

func ExamplePipeline_reverse() {
    runner := Pipeline(&Question{}, &Upper{})
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err := testRun(runner, data)
    fmt.Println(data, err)

    // Output: [[IS HELLO? IS WORLD?]] <nil>
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipelineErr(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    infinity := &InfinityRunner{}
    runner := Pipeline(infinity, &ErrRunner{err})
    data := NewDataset(Null.Data(1))
    data, err = testRun(runner, data)

    require.Equal(t, 0, data.Width())
    require.Error(t, err)
    require.Equal(t, "something bad happened", err.Error())
    require.Equal(t, false, infinity.Running, "Infinity go-routine leak")
}


func TestPipelineReturnsWildcard(t *testing.T) {
    runner := Project(&Upper{}, &Question{})
    runner = Pipeline(runner, PassThrough())
    types := runner.Returns()
    require.Equal(t, 2, len(types))
    require.Equal(t, Str.Name(), types[0].Name())
    require.Equal(t, Str.Name(), types[1].Name())

    runner = Project(&Upper{}, &Question{})
    runner = Pipeline(runner, Pick(1))
    types = runner.Returns()
    require.Equal(t, 1, len(types))
}

func TestPipelineReturnsWildcardIdx(t *testing.T) {
    runner := Project(&Upper{}, &Question{})
    runner = Pipeline(runner, Pick(1))
    types := runner.Returns()
    require.Equal(t, 1, len(types))
    require.Equal(t, Str.Name(), types[0].Name())
}
