package ep

// Types utility registry. Use Type.Add() and Type.Get()
var Types = typesReg{}
var Runners = runnersReg{}

type typesReg map[string]Type
func (reg typesReg) Get(name string) Type { return reg[name] }
func (reg typesReg) Add(name string, t Type) int {
    reg[name] = t
    return len(reg)
}

type runnersReg map[string]Runner
func (reg runnersReg) Get(name string) Runner { return reg[name] }
func (reg runnersReg) Add(name string, r Runner) int {
    reg[name] = r
    return len(reg)
}
