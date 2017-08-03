package ep

import (
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
func TestProjectErr(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &infinityRunner{}
	runner := Project(infinity, &errRunner{err})
	data := NewDataset(Null.Data(1))
	data, err = TestRunner(runner, data)

	require.Equal(t, 0, data.Width())
	require.Error(t, err)
	require.Equal(t, "something bad happened", err.Error())
	require.Equal(t, false, infinity.Running, "Infinity go-routine leak")
}

// Test that Projected runners always returns the same number of rows
func TestProjectMismatchErr(t *testing.T) {
	runner := Project(&upper{}, &count{})
	data := NewDataset(strs([]string{"hello", "world"}))
	_, err := TestRunner(runner, data)
	require.Error(t, err)
}
