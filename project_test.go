package ep

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func ExampleProject() {
	runner := Project(&Upper{}, &Question{})
	data := NewDataset(Strs([]string{"hello", "world"}))
	data, err := testRun(runner, data)
	fmt.Println(data, err)

	// Output:
	// [[HELLO WORLD] [is hello? is world?]] <nil>
}

func ExampleProject_reversed() {
	runner := Project(&Question{}, &Upper{})
	data := NewDataset(Strs([]string{"hello", "world"}))
	data, err := testRun(runner, data)
	fmt.Println(data, err)

	// Output:
	// [[is hello? is world?] [HELLO WORLD]] <nil>
}

// project error should cancel all inner runners
func TestProjectErr(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &InfinityRunner{}
	runner := Project(infinity, &ErrRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = testRun(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.Running, "Infinity go-routine leak")
}

// Test how Project behaves on a mismatch in the number of columns
func TestProjectMismatch(t *testing.T) {
	runner := Project(&Upper{}, &Count{})
	data := NewDataset(Strs([]string{"hello", "world"}))
	data, err := testRun(runner, data)
	require.NoError(t, err)

	fmt.Println("ROI", data)
}
