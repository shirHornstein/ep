package ep

import (
    "fmt"
    "testing"
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

// test that a top-level error doesn't block cancels lower-level runners
func TestErrOnTop(t *testing.T) {
    err := fmt.Errorf("something bad happened")
    runner := Pipeline(&Question{}, &ErrRunner{err})
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err = testRun(runner, data)
    fmt.Println(data, err)
}
