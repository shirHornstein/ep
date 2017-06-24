package ep
//
// import (
//     "fmt"
//     "net"
// )
//
// type transport struct { net.Listener }
// func (*transport) Dial(addr string) (net.Conn, error) {
//     return net.Dial("tcp", addr)
// }
//
// func ExampleScatter() {
//     ln1, _ := net.Listen("tcp", ":5551")
//     dist1 := NewDistributer(&transport{ln1})
//     go dist1.Start()
//
//     ln2, _ := net.Listen("tcp", ":5552")
//     dist2 := NewDistributer(&transport{ln2})
//     go dist2.Start()
//
//     data1 := NewDataset(Strs{"hello", "world"})
//     data2 := NewDataset(Strs{"foo", "bar"})
//     runner := Pipeline(PassThrough(), Scatter())
//     runner, err := dist1.Distribute(runner, ":5551", ":5551", ":5552")
//     if err != nil { panic(err) }
//
//     data, err := testRun(runner, data1, data2)
//     fmt.Println(data, err)
//
//     // Output: [[hello world foo bar]] <nil>
// }
