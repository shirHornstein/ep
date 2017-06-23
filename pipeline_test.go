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

// errors at the bottom of the pipeline should propagate upwards
func TestPipelineErrOnBottom(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    runner := Pipeline(&ErrRunner{err}, PassThrough())
    data := NewDataset(Null.Data(1))
    data, err = testRun(runner, data)

    require.Equal(t, 0, data.Width())
    require.Error(t, err)
    require.Equal(t, "something bad happened", err.Error())
}

// test that a top-level error doesn't block
func TestPipelineErrOnTop(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    runner := Pipeline(PassThrough(), &ErrRunner{err})
    data := NewDataset(Null.Data(1))
    data, err = testRun(runner, data)

    require.Equal(t, 0, data.Width())
    require.Error(t, err)
    require.Equal(t, "something bad happened", err.Error())
}

// test that upon an error, the producing (infinity) runners are canceled.
// Otherwise - this test will block indefinitely
func TestPipelineErrCancel(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    runner := Pipeline(&InfinityRunner{}, &ErrRunner{err})
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err = testRun(runner, data)

    require.Equal(t, 0, data.Width())
    require.Error(t, err)
    require.Equal(t, "something bad happened", err.Error())
}
