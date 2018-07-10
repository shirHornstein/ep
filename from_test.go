package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func ExampleFrom() {
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	r := ep.From(1, data)
	data, err := eptest.Run(r)

	fmt.Println(data, err)

	// Output:
	// [[hello world]] <nil>
}

func ExampleFrom_multi() {
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	r := ep.From(2, data)
	data, err := eptest.Run(r)

	fmt.Println(data, err)

	// Output:
	// [[hello world hello world]] <nil>
}

func TestFrom_distributed(t *testing.T) {
	ln1, _ := net.Listen("tcp", ":5551")
	dist1 := ep.NewDistributer(":5551", ln1)
	defer dist1.Close()

	ln2, _ := net.Listen("tcp", ":5552")
	dist2 := ep.NewDistributer(":5552", ln2)
	defer dist2.Close()

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	runner := ep.From(1, data1, data2)

	runner = dist1.Distribute(runner, ":5551", ":5552")

	data, err := eptest.Run(runner, data1, data2)
	require.NoError(t, err)

	// no gather - only one batch should return
	require.Equal(t, []string{"[hello world]"}, data.Strings())
}
