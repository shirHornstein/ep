package ep

import (
    "fmt"
    "net"
    "testing"
    "github.com/stretchr/testify/require"
)

func ExampleScatter_single() {
    ln, _ := net.Listen("tcp", ":5551")
    dist := NewDistributer(":5551", ln)
    go dist.Start()
    defer dist.Close()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner, _ := dist.Distribute(Scatter(), ":5551", ":5551")
    data, err := testRun(runner, data1, data2)
    fmt.Println(data, err)

    // Output: [[hello world foo bar]] <nil>
}

// Test that errors are transmitted across the network (an error in one node
// is reported to the calling node).
func TestExchangeErr(t *testing.T) {
    ln1, err := net.Listen("tcp", ":5551")
    require.NoError(t, err)

    dist1 := NewDistributer(":5551", ln1)
    defer dist1.Close()
    go dist1.Start()

    ln2, err := net.Listen("tcp", ":5552")
    require.NoError(t, err)

    dialer := &errDialer{ln2, fmt.Errorf("bad connection")}
    dist2 := NewDistributer(":5552", dialer)
    defer dist2.Close()
    go dist2.Start()

    ln3, err := net.Listen("tcp", ":5553")
    require.NoError(t, err)

    dist3 := NewDistributer(":5553", ln3)
    defer dist3.Close()
    go dist3.Start()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner, err := dist1.Distribute(Scatter(), ":5551", ":5551", ":5552", ":5553")
    require.NoError(t, err)

    data, err := testRun(runner, data1, data2)
    require.Equal(t, 0, data.Width())
    require.Error(t, err)
    require.Equal(t, "bad connection", err.Error())
}

type errDialer struct { net.Listener; Err error }
func (e *errDialer) Dial(addr string) (net.Conn, error) { return nil, e.Err }

// Tests the scattering when there's just one node - the whole thing should
// be short-circuited to act as a pass-through
func TestScatterSingleNode(t *testing.T) {
    ln, err := net.Listen("tcp", ":5551")
    require.NoError(t, err)

    dist := NewDistributer(":5551", ln)
    go dist.Start()
    defer dist.Close()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner, err := dist.Distribute(Scatter(), ":5551", ":5551")
    require.NoError(t, err)

    data, err := testRun(runner, data1, data2)
    require.NoError(t, err)
    require.Equal(t, 1, data.Width())
    require.Equal(t, 4, data.Len())
}


// func TestScatterMulti(t *testing.T) {
//     ln1, err := net.Listen("tcp", ":5551")
//     require.NoError(t, err)
//
//     dist1 := NewDistributer(":5551", ln1)
//     defer dist1.Close()
//     go dist1.Start()
//
//     ln2, err := net.Listen("tcp", ":5552")
//     require.NoError(t, err)
//
//     dist2 := NewDistributer(":5552", ln2)
//     defer dist2.Close()
//     go dist2.Start()
//
//     data1 := NewDataset(Strs{"hello", "world"})
//     data2 := NewDataset(Strs{"foo", "bar"})
//     runner, err := dist1.Distribute(Scatter(), ":5551", ":5551", ":5552")
//     require.NoError(t, err)
//
//     data, err := testRun(runner, data1, data2)
//     require.NoError(t, err)
//
//     fmt.Println(data)
// }
