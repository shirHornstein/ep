package ep

import (
    "io"
    "context"
)

// Node is an interface representing a peer node in the cluster that's
// destignated to participate in the work of running distributed Runners. It
// provides the API required for communicating with the node.
type Node interface {

    // Connect to the node, denoted by a special UUID. It MUST be the case that
    // the same UUID would always return the same connection, on both sides of
    // the connection. The UUID is the mechanism used to bind the two nodes in
    // a specific logic location of the code, and thus must be consistent.
    // When the connection is closed, it's safe to assume that no additional
    // Connect() calls will arrive for the same UUID, thus its mapping can be
    // safely removed from memory.
    Connect(uuid string) (net.Conn, error)

    // IsCurrentNode determines if this node is the current node running the
    // code. When executed on different nodes, only one of them should return
    // true for this
    IsCurrentNode() bool
}
