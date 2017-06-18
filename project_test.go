package ep

import (
    "fmt"
    "context"
)

func ExampleProject() {
    runner := Project(&Upper{}, &Question{})
    inp := make(chan Dataset, 1)
    inp <- NewDataset(Strs([]string{"hello", "world"}))
    close(inp)

    out := make(chan Dataset)
    go func() {
        runner.Run(context.Background(), inp, out)
        close(out)
    }()

    for data := range out {
        fmt.Println(data)
    }

    // Output:
    // [[HELLO WORLD] [is hello? is world?]]
}
