package ep

import (
    "io"
    "context"
    "encoding/gob"
    "container/ring"
)

// Node is an interface representing a peer node in the cluster that's
// destignated to participate in the work of running distributed Runners. It
// provides the API required for communicating with the node.
type Node interface {

    // Connect to the node, denoted by a unique ID. It MUST be the case that
    // the same UID will always return the same connection, on both sides of
    // the connection. The UID is the mechanism used to bind the two nodes in
    // a specific logicical location in the code, and thus must be consistent.
    //
    // In other words - when Connect is called from NodaA to NodeB, yielding
    // Connection1, it might also be called concurrently from NodeB to NodeA,
    // and should return the same Connection1.
    //
    // When the connection is closed, it's safe to assume that no additional
    // Connect() calls will arrive for the same UID, thus its mapping can be
    // safely removed from memory.
    // Connect(uid string) (net.Conn, error)
    Address() string
}
