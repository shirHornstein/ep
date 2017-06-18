package ep

import (
    "fmt"
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
