package ep

import (
    "fmt"
    "context"
)

func ExamplePipeline() {
    runner := Pipeline(&Upper{}, &Question{})
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

    // Output: [[is HELLO? is WORLD?]]

}

func ExamplePipeline_reverse() {
    runner := Pipeline(&Question{}, &Upper{})
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

    // Output: [[IS HELLO? IS WORLD?]]
}
