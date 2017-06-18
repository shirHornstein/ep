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
        fmt.Println(data.At(0)) // Output: [is HELLO? is WORLD?]
    }

}

func ExamplePipeline_Reverse() {
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
        fmt.Println(data.At(0)) // Output: [IS HELLO? IS WORLD?]
    }
    
}
