// Distributed Query Processing & Execution Framework. Short for (and
// pronounced) Epsilon, ep is designed to make it easy to construct complex
// query engines and data processing pipelines that are distributed across a
// cluster of nodes.
package ep

import (
    "encoding/gob"
)

func registerGob(es ...inteface{}) bool {
    for _, e := range es {
        gob.Register(e)
    }
    return true
}
