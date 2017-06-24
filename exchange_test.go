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


func TestScatterMulti(t *testing.T) {
    ln1, err := net.Listen("tcp", ":5551")
    require.NoError(t, err)

    dist1 := NewDistributer(":5551", ln1)
    defer dist1.Close()
    go dist1.Start()

    ln2, err := net.Listen("tcp", ":5552")
    require.NoError(t, err)

    dist2 := NewDistributer(":5552", ln2)
    defer dist2.Close()
    go dist2.Start()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner, err := dist1.Distribute(Scatter(), ":5551", ":5551", ":5552")
    require.NoError(t, err)

    data, err := testRun(runner, data1, data2)
    require.NoError(t, err)

    fmt.Println(data)
}
