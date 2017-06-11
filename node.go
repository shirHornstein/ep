package ep

import (
    "io"
    "context"
)

// Node is an interface representing a peer node in the cluster that's
// destignated to participate in the work of running distributed Runners. It
// provides the API required for communicating with the node.
type Node interface {

}


type conn struct {
    net.Conn
    Enc *gob.Encoder
    Dec *gob.Decoder
}

// conns is a helper object that represents a set of network connections to
// peer nodes. It provides the API for sending and receiving data from multiple
// nodes at once
type conns *container.Ring

// By default - send to all
func (c conns) SendAll(data Dataset) (err error) {
    for i := 0 ; i < c.Len() ; i++ {
        conn := c.Next().Value.(*conn)
        err1 := conn.Enc.Encode(data)
        if err != nil {
            err = err1
        }
    }
}

// send round robin
func (c conns) Send(data Dataset) error {
    conn := c.Next().Value.(*conn)
    return conn.Enc.Encode(data)
}

// receive round robin
func (c conns) Receive() (Dataset, error) {
    if c.Len() == 0 {
        return nil, io.EOF
    }

    // receive round robin
    conn := c.Next().Value.(*conn)

    var data Dataset
    err := conn.Dec.Decode(data)
    if err == io.EOF {
        // current connection is depleted, remove it and retry
        conns.Unlink(-1)
        return c.Receive()
    } else if err != nil {
        return nil, err
    }

    return data, nil
}

func (cs *conns) Close(err error) (err error) {
    for i := 0 ; i < c.Len() ; i++ {
        conn := c.Next().Value.(*conn)
        err1 := conn.Close()
        if err1 != nil {
            err = err1
        }
    }
}
