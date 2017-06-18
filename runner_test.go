package ep

import (
    "fmt"
    "context"
    "strings"
)

type Upper struct {}
func (*Upper) Returns() []Type { return []Type{Str} }
func (*Upper) Run(_ context.Context, inp, out chan Dataset) error {
    for data := range inp {
        res := make(Strs, data.Len())
        for i, v := range data.At(0).(Strs) {
            res[i] = strings.ToUpper(v)
        }
        out <- NewDataset(res)
    }
    return nil
}

type Question struct {}
func (*Question) Returns() []Type { return []Type{Str} }
func (*Question) Run(_ context.Context, inp, out chan Dataset) error {
    for data := range inp {
        res := make(Strs, data.Len())
        for i, v := range data.At(0).(Strs) {
            res[i] = "is " + v + "?"
        }
        out <- NewDataset(res)
    }
    return nil
}

func ExampleRunner() {
    upper := &Upper{}
    inp := make(chan Dataset, 1)
    inp <- NewDataset(Strs([]string{"hello", "world"}))
    close(inp)

    out := make(chan Dataset)
    go func() {
        upper.Run(context.Background(), inp, out)
        close(out)
    }()

    for data := range out {
        fmt.Println(data)
    }

    // Output: [[HELLO WORLD]]
}
