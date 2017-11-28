package ep

import (
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
func TestPipeline_err(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &infinityRunner{}
	runner := Pipeline(infinity, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.Running, "Infinity go-routine leak")
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
