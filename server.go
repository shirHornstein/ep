package ep

import (
    "net"
)

type Server interface {
    Serve(net.Conn) error
}

// Creates a new server that can accept and execute distributed Runners.
func NewServer() Server {
    return &server{n}
}

type server Node

func (s *server) Serve(conn net.Conn) error {
    return nil
}
