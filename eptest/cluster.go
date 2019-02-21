package eptest

// cluster.go contains utilities for writing tests with clusters

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

// NewPeer returns distributer that listens on the given port
func NewPeer(t *testing.T, port string) ep.Distributer {
	ln, err := net.Listen("tcp", port)
	require.NoError(t, err)
	return ep.NewDistributer(port, ln)
}

// ClosePeer closes all given distributers
func ClosePeer(t *testing.T, dist ep.Distributer) {
	// use assert and not require to make sure all dists will be closed
	assert.NoError(t, dist.Close())
}

// NewDialingErrorPeer returns distributer that fails to .Dial()
func NewDialingErrorPeer(t *testing.T, port string) ep.Distributer {
	ln, err := net.Listen("tcp", port)
	require.NoError(t, err)
	dialer := &errDialer{ln, fmt.Errorf("bad connection from port %s", port)}
	return ep.NewDistributer(port, dialer)
}

type errDialer struct {
	net.Listener
	Err error
}

func (e *errDialer) Dial(net, addr string) (net.Conn, error) {
	return nil, e.Err
}

// RunDist is like Run, but first distributes the runner to n nodes, starting
// from address ":5551"
func RunDist(t *testing.T, n int, r ep.Runner, datasets ...ep.Dataset) (ep.Dataset, error) {
	var master ep.Distributer
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		port := fmt.Sprintf(":%04d", 5551+i) // 5551, 5552, 5553, ...
		ports[i] = port

		dist := NewPeer(t, port)
		if i == 0 {
			master = dist
		}
		defer ClosePeer(t, dist)
	}

	r = ep.Pipeline(r, ep.Gather())
	r = master.Distribute(r, ports...)
	return Run(r, datasets...)
}
