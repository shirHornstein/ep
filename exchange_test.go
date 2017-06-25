package ep

import (
    "fmt"
    "net"
    "time"
    "testing"
    "github.com/stretchr/testify/require"
)

type errDialer struct { net.Listener; Err error }
func (e *errDialer) Dial(addr string) (net.Conn, error) { return nil, e.Err }

func ExampleScatter() {
    ln1, _ := net.Listen("tcp", ":5551")
    dist1 := NewDistributer(":5551", ln1)
    go dist1.Start()
    defer dist1.Close()

    ln2, _ := net.Listen("tcp", ":5552")
    dist2 := NewDistributer(":5552", ln2)
    go dist2.Start()
    defer dist2.Close()

    data1 := NewDataset(Strs{"hello", "world"})
    data2 := NewDataset(Strs{"foo", "bar"})
    runner, _ := dist1.Distribute(Scatter(), ":5551", ":5551", ":5552")
    data, err := testRun(runner, data1, data2)
    fmt.Println(data, err) // no gather - only one batch should return

    // Output: [[foo bar]] <nil>
}

// Test that errors are transmitted across the network (an error in one node
// is reported to the calling node).
func TestExchangeErr(t *testing.T) {
    defer time.Sleep(1 * time.Millisecond) // bind: address already in use

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
