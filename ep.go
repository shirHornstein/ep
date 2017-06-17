// Distributed Query Processing & Execution Framework. Short for (and
// pronounced) Epsilon, ep is designed to make it easy to construct complex
// query engines and data processing pipelines that are distributed across a
// cluster of nodes.
//
// Registries
//
// In order to support modular systems design, ep ships with a builtin
// registries for Runners and Types. These registries support simple Set and Get
// functions:
//
//      func (reg typesReg) Get(name string) Type
//      func (reg typesReg) Add(name string, t Type) int
//
package ep

import (
    "encoding/gob"
)

func registerGob(es ...interface{}) bool {
    for _, e := range es {
        gob.Register(e)
    }
    return true
}
