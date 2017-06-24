package ep

import (
    "fmt"
)

func ExampleScatter() {
    runner := PassThrough()
    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    data, err := testRun(runner, data1, data2)
    fmt.Println(data, err)

    // Output: [[hello world foo bar]] <nil>
}
