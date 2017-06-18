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
    data := NewDataset(Strs([]string{"hello", "world"}))
    data, err := testRun(nil, upper, data)
    fmt.Println(data, err)

    // Output: [[HELLO WORLD]] <nil>
}

// run a runner with the given input to completion
func testRun(ctx context.Context, r Runner, data Dataset) (Dataset, error) {
    var err error

    inp := make(chan Dataset, 1)
    inp <- data
    close(inp)

    out := make(chan Dataset)
    go func() {
        err = r.Run(context.Background(), inp, out)
        close(out)
    }()

    var res = NewDataset()
    for data := range out {
        res = res.Append(data).(Dataset)
    }

    return res, err
}
