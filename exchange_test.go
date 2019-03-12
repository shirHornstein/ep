package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"sort"
	"strings"
	"testing"
	"time"
)

// Example of Scatter with just 2 nodes. The datasets are scattered in
// round-robin to the two nodes such that each node receives half of the
// datasets. Thus the output in the local node just returns half of the output.
// Pipelining into a Gather runner would recollected the scattered outputs
func ExampleScatter() {
	ln1, _ := net.Listen("tcp", ":5551")
	dist1 := ep.NewDistributer(":5551", ln1)
	defer dist1.Close()

	ln2, _ := net.Listen("tcp", ":5552")
	dist2 := ep.NewDistributer(":5552", ln2)
	defer dist2.Close()

	runner := dist1.Distribute(ep.Scatter(), ":5551", ":5552")

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)
	fmt.Println(data.Strings(), err) // no gather - only dist1 data should return

	// Output:
	// [(world) (bar)] <nil>
}

func TestExchange_dialingError(t *testing.T) {
	port1 := ":5551"
	dist1 := eptest.NewPeer(t, port1)

	port2 := ":5552"
	peer2 := eptest.NewDialingErrorPeer(t, port2)

	port3 := ":5553"
	peer3 := eptest.NewPeer(t, port3)
	defer func() {
		eptest.ClosePeer(t, dist1)
		eptest.ClosePeer(t, peer2)
		eptest.ClosePeer(t, peer3)
	}()

	runner := dist1.Distribute(ep.Scatter(), port1, port2, port3)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	_, err := eptest.Run(runner, data1, data2)

	require.Error(t, err)

	possibleErrors := []string{
		// when interacting with peers after their failure
		"write tcp",
		"read tcp",

		"bad connection from port :5552",        // reported by 5552, when dialing to :5553
		"ep: connect timeout; no incoming conn", // reported by 5553, when waiting to :5552
	}
	errMsg := err.Error()
	isExpectedError := strings.Contains(errMsg, possibleErrors[0]) ||
		strings.Contains(errMsg, possibleErrors[1]) ||
		errMsg == possibleErrors[2] ||
		errMsg == possibleErrors[3]
	require.True(t, isExpectedError, "expected \"%s\" to appear in %s", err.Error(), possibleErrors)
}

// Tests the scattering when there's just one node - the whole thing should
// be short-circuited to act as a pass-through
func TestScatter_singleNode(t *testing.T) {
	port := ":5551"
	dist := eptest.NewPeer(t, port)
	defer eptest.ClosePeer(t, dist)

	runner := dist.Distribute(ep.Scatter(), port)

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.Run(runner, data1, data2)
	require.NoError(t, err)
	require.Equal(t, 1, data.Width())
	require.Equal(t, 4, data.Len())
}

func TestScatter_multipleNodes(t *testing.T) {
	t.Run("less data than peers", func(t *testing.T) {
		col1 := strs{"a1", "b1"}
		col2 := strs{"a2", "b2"}
		col3 := strs{"a3", "b3"}
		col4 := strs{"a4", "b4"}
		col5 := strs{"a5", "b5"}
		col6 := strs{"a6", "b6"}

		data := ep.NewDataset(col1, col2, col3, col4, col5, col6)

		runner := ep.Pipeline(ep.Scatter(), &nodeAddr{})
		res, err := eptest.RunDist(t, 3, runner, data)
		require.NoError(t, err)

		sort.Sort(res)

		expectedOutput := []string{
			"(a1,a2,a3,a4,a5,a6,:5552)",
			"(b1,b2,b3,b4,b5,b6,:5553)",
		}

		require.Equal(t, expectedOutput, res.Strings())
	})

	t.Run("same batch size to all peers", func(t *testing.T) {
		col1 := strs{"a1", "b1", "c1", "d1", "e1", "f1"}
		col2 := strs{"a2", "b2", "c2", "d2", "e2", "f2"}
		col3 := strs{"a3", "b3", "c3", "d3", "e3", "f3"}
		col4 := strs{"a4", "b4", "c4", "d4", "e4", "f4"}
		col5 := strs{"a5", "b5", "c5", "d5", "e5", "f5"}
		col6 := strs{"a6", "b6", "c6", "d6", "e6", "f6"}

		data := ep.NewDataset(col1, col2, col3, col4, col5, col6)

		runner := ep.Pipeline(ep.Scatter(), &nodeAddr{})
		res, err := eptest.RunDist(t, 3, runner, data)
		require.NoError(t, err)

		sort.Sort(res)

		expectedOutput := []string{
			"(a1,a2,a3,a4,a5,a6,:5552)",
			"(b1,b2,b3,b4,b5,b6,:5552)",
			"(c1,c2,c3,c4,c5,c6,:5553)",
			"(d1,d2,d3,d4,d5,d6,:5553)",
			"(e1,e2,e3,e4,e5,e6,:5551)",
			"(f1,f2,f3,f4,f5,f6,:5551)",
		}

		require.Equal(t, expectedOutput, res.Strings())
	})

	t.Run("different batch size to all peers", func(t *testing.T) {
		col1 := strs{"a1", "b1", "c1", "d1", "e1"}
		col2 := strs{"a2", "b2", "c2", "d2", "e2"}
		col3 := strs{"a3", "b3", "c3", "d3", "e3"}
		col4 := strs{"a4", "b4", "c4", "d4", "e4"}
		col5 := strs{"a5", "b5", "c5", "d5", "e5"}
		col6 := strs{"a6", "b6", "c6", "d6", "e6"}

		data := ep.NewDataset(col1, col2, col3, col4, col5, col6)

		runner := ep.Pipeline(ep.Scatter(), &nodeAddr{})
		res, err := eptest.RunDist(t, 3, runner, data)
		require.NoError(t, err)

		sort.Sort(res)

		expectedOutput := []string{
			"(a1,a2,a3,a4,a5,a6,:5552)",
			"(b1,b2,b3,b4,b5,b6,:5552)",
			"(c1,c2,c3,c4,c5,c6,:5553)",
			"(d1,d2,d3,d4,d5,d6,:5553)",
			"(e1,e2,e3,e4,e5,e6,:5551)",
		}

		require.Equal(t, expectedOutput, res.Strings())
	})
}

func TestScatter(t *testing.T) {
	runner := ep.Pipeline(ep.Scatter(), &nodeAddr{})

	data1 := ep.NewDataset(strs{"hello", "world"})
	data2 := ep.NewDataset(strs{"foo", "bar"})
	data, err := eptest.RunDist(t, 3, runner, data1, data2)

	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "[[hello world foo bar] [:5552 :5553 :5551 :5552]]", fmt.Sprintf("%v", data))
}

func TestPartition_and_Gather(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	maxPort := 7000
	minPort := 6000
	randomPort := rand.Intn(maxPort-minPort) + minPort

	port1 := fmt.Sprintf(":%d", randomPort)
	peer1 := eptest.NewPeer(t, port1)
	defer eptest.ClosePeer(t, peer1)

	port2 := fmt.Sprintf(":%d", randomPort+1)
	peer2 := eptest.NewPeer(t, port2)
	defer eptest.ClosePeer(t, peer2)

	runner := ep.Pipeline(ep.Partition(0), ep.PassThrough(), ep.Gather())
	runner = peer1.Distribute(runner, port1, port2)

	firstColumn := strs{"this", "is", "sparta"}
	secondColumn := strs{"meh", "shtoot", "nya"}

	data := ep.NewDataset(firstColumn, secondColumn)
	res, err := eptest.Run(runner, data)

	require.NoError(t, err)
	require.NotNil(t, res)

	// partition->gather does not ensure the same order of entries
	require.ElementsMatch(t, firstColumn, res.At(0))
	require.ElementsMatch(t, secondColumn, res.At(1))
}

func TestPartition(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		col1 := strs{"one-1", "one-1", "two-2", "two-2", "", "", "a", "a"}
		col1.MarkNull(4)
		col1.MarkNull(5)

		col2 := strs{"two-2", "meh", "one-1", "moo", "a", "", "", "b"}
		col2.MarkNull(5)
		col2.MarkNull(6)

		data := ep.NewDataset(col1, col2)

		runner := ep.Pipeline(ep.Partition(0), &nodeAddr{})
		res, err := eptest.RunDist(t, 2, runner, data)
		require.NoError(t, err)

		sort.Sort(res)

		// the following output is valid for the current algorithm only
		// when the first col is the same, data arrives to the same node
		expectedOutput := []string{
			"(,,:5551)", "(,a,:5551)",
			"(a,,:5551)", "(a,b,:5551)",
			"(one-1,meh,:5551)", "(one-1,two-2,:5551)",
			"(two-2,moo,:5552)", "(two-2,one-1,:5552)",
		}

		require.Equal(t, expectedOutput, res.Strings())
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		col1 := strs{
			"10", "20", "", "10",
			"10", "20", "", "10",
		}
		col1.MarkNull(2)
		col1.MarkNull(5)

		col2 := strs{
			"", "10", "", "20",
			"", "10", "", "20",
		}
		col2.MarkNull(0)
		col2.MarkNull(2)
		col2.MarkNull(3)
		col2.MarkNull(5)

		col3 := strs{
			"a", "b", "c", "d",
			"e", "f", "g", "h",
		}

		data := ep.NewDataset(col1, col2, col3)

		runner := ep.Pipeline(ep.Partition(0, 1), &nodeAddr{})
		res, err := eptest.RunDist(t, 2, runner, data)
		require.NoError(t, err)

		sort.Sort(res)

		// the following output is valid for the current algorithm only
		// when first two columns are the same, data is on the same node
		expectedOutput := []string{
			"(,,c,:5551)", "(,,g,:5551)",
			"(10,,a,:5551)", "(10,,e,:5551)",
			"(10,20,d,:5551)", "(10,20,h,:5551)",
			"(20,10,b,:5552)", "(20,10,f,:5552)",
		}

		require.Equal(t, expectedOutput, res.Strings())
	})

	t.Run("sends complete datasets", func(t *testing.T) {
		firstColumn := strs{"foo", "bar", "meh", "nya", "shtoot", "a", "few", "more", "things"}
		secondColumn := strs{"f", "s", "f", "f", "s", "f", "f", "f", "s"}

		data := ep.NewDataset(firstColumn, secondColumn)
		runner := ep.Pipeline(ep.Partition(1), &count{})

		res, err := eptest.RunDist(t, 2, runner, data)
		require.NoError(t, err)

		// there are 6 "f" and 3 "s" in second column which is used for partitioning
		expected := []string{"6", "3"}
		sizes := res.At(0)

		require.Equal(t, 2, sizes.Len())
		require.ElementsMatch(t, expected, sizes.Strings())
	})
}

// test that exchange runners act as passThrough when executed without a
// distributer
func TestExchange_undistributed(t *testing.T) {
	data := ep.NewDataset(strs{"hello", "world"})
	data, err := eptest.Run(ep.Scatter(), data)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, []string{"(hello)", "(world)"}, data.Strings())

	data, err = eptest.Run(ep.Broadcast(), data)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, []string{"(hello)", "(world)"}, data.Strings())

	data, err = eptest.Run(ep.Gather(), data)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, []string{"(hello)", "(world)"}, data.Strings())

	data, err = eptest.Run(ep.Partition(0), data)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, []string{"(hello)", "(world)"}, data.Strings())
}
