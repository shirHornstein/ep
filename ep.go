// Distributed Query Processing & Execution Framework. Short for (and
// pronounced) Epsilon, ep is designed to make it easy to construct complex
// query engines and data processing pipelines that are distributed across a
// cluster of nodes.
//
// Registries
//
// In order to support modular systems design, ep ships with a builtin
// registries for Runners and Types. These registries support simple Set and Get
// operations:
//
//      func (typesReg) Add(name string, t Type) int
//      func (typesReg) Get(name string) Type
//
// And for Runners:
//
//      func (runnersReg) Get(name string) []Runner
//      func (runnersReg) Add(name string, r Runner) int
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

var Types = typesReg{} // see Registries above.
var Runners = runnersReg{} // see Registries above.

type typesReg map[string]Type
func (reg typesReg) Get(name string) Type { return reg[name] }
func (reg typesReg) Add(name string, t Type) int {
    registerGob(t)
    reg[name] = t
    return len(reg)
}

type runnersReg map[string][]Runner
func (reg runnersReg) Get(name string) []Runner { return reg[name] }
func (reg runnersReg) Add(name string, r Runner) int {
    registerGob(r)
    if reg[name] == nil {
        reg[name] = []Runner{}
    }

    reg[name] = append(reg[name], r)
    return len(reg)
}
