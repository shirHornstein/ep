package ep

import (
    "fmt"
    "context"
)

func ExampleUnion() {
    runner := Union(&Upper{}, &Question{})
    inp := make(chan Dataset, 1)
    inp <- NewDataset(Strs([]string{"hello", "world"}))
    close(inp)

    out := make(chan Dataset)
    go func() {
        runner.Run(context.Background(), inp, out)
        close(out)
    }()

    var res = NewDataset()
    for data := range out {
        res = res.Append(data).(Dataset)
    }

    fmt.Println(res)

    // Output:
    // [[HELLO WORLD is hello? is world?]]
}
