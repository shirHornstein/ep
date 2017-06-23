package ep

import (
    "fmt"
    "testing"
    "github.com/stretchr/testify/require"
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

// project should cancel
func TestProjectErr(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    infinity := &InfinityRunner{}
    runner := Project(infinity, &ErrRunner{err})
    data := NewDataset(Null.Data(1))
    data, err = testRun(runner, data)

    require.Error(t, err)
    require.Equal(t, "something bad happened", err.Error())
    require.Equal(t, false, infinity.Running, "Infinity go-routine leak")

    // fmt.Println(data, err)
}
