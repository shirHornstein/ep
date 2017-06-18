package ep

import (
    "fmt"
)

func ExampleProject() {
    runner := Project(&Upper{}, &Question{})
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err := testRun(runner, data)
    fmt.Println(data, err)

    // Output:
    // [[HELLO WORLD] [is hello? is world?]] <nil>
}
