package eptest

// Cluster utils for writing tests with clusters

import (
	"fmt"
	"github.com/panoplyio/ep"
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
