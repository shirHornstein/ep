package ep

import (
    "fmt"
    "net"
    "testing"
)

func ExampleScatter_single() {
    ln, _ := net.Listen("tcp", ":5551")
    dist := NewDistributer(ln)
    go dist.Start()
    defer dist.Close()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner := Pipeline(PassThrough(), Scatter())
    runner, _ = dist.Distribute(runner, ":5551", ":5551")
    data, err := testRun(runner, data1, data2)
    fmt.Println(data, err)

    // Output: [[hello world foo bar]] <nil>
}


func TestScatterMulti(t *testing.T) {
    ln, _ := net.Listen("tcp", ":5551")
    dist := NewDistributer(ln)
    defer dist.Close()
    go dist.Start()
//
//     data1 := NewDataset(Strs{"hello", "world"})
//     data2 := NewDataset(Strs{"foo", "bar"})
//     runner := Pipeline(PassThrough(), Scatter())
//     runner, err := dist.Distribute(runner, ":5551", ":5551")
//     if err != nil { panic(err) }
//
//     data, err := testRun(runner, data1, data2)
//     if err != nil { panic(err) }
//     fmt.Println(data, err)
}
