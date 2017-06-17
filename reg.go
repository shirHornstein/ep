package ep

var Types = typesReg{} // see Registries above.
var Runners = runnersReg{} // see Registries above.

type typesReg map[string]Type
func (reg typesReg) Get(name string) Type { return reg[name] }
func (reg typesReg) Add(name string, t Type) int {
    reg[name] = t
    return len(reg)
}

type runnersReg map[string][]Runner
func (reg runnersReg) Get(name string) []Runner { return reg[name] }
func (reg runnersReg) Add(name string, r Runner) int {
    if reg[name] == nil {
        reg[name] = []Runner{}
    }

    reg[name] = append(reg[name], r)
    return len(reg)
}
