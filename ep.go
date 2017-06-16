// Distributed Query Processing & Execution Framework. Short for (and
// pronounced) Epsilon, ep is designed to make it easy to construct complex
// query engines and data processing pipelines that are distributed across a
// cluster of nodes.
package ep

import (
    "encoding/gob"
)

var repo = map[string]Runner{}

// Add a Runner to the global repository by name
func Add(name string, r Runner) Runner {
    repo[name] = r
    return r
}

// Get a Runner by the global repository
func Get(name string) Runner {
    return repo[name]
}

func registerGob(es ...interface{}) bool {
    for _, e := range es {
        gob.Register(e)
    }
    return true
}
