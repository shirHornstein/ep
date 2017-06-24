package ep

import (
    "fmt"
    "net"
)

func ExampleScatter_single() {
    ln, _ := net.Listen("tcp", ":5551")
    dist := NewDistributer(ln)
    go dist.Start()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner := Pipeline(PassThrough(), Scatter())
    runner, err := dist.Distribute(runner, ":5551", ":5551")
    data, err := testRun(runner, data1, data2)
    fmt.Println(data, err)

    // Output: [[hello world foo bar]] <nil>
}
