package ep

import (
    "fmt"
    "reflect"
    "context"
)

var Runners = make(runnersReg)
var Types = make(typesReg)

// Plan a new Runner marked by an arbitrary argument that must've been
// preregistered using the `Runners.Register()` function. See Planning above.
func Plan(ctx context.Context, arg interface{}) (Runner, error) {
    var err error
    for _, r := range Runners.Get(arg) {

        // check if the runner is plannable
        p, ok := r.(RunnerPlan)
        if !ok {
            // not a plannable Runner, return it as-is.
            return r, nil
        }

        // otherwise - let it plan itself
        r, err = p.Plan(ctx, arg)
        if err == nil {
            return r, nil
        }
    }

    if err == nil {
        err = fmt.Errorf("Unsupported")
    }

    return nil, err
}

// registry of runners
type runnersReg map[interface{}][]Runner
func (reg runnersReg) Register(k interface{}, r Runner) runnersReg {
    registerGob(r)
    k = registryKey(k)
    reg[k] = append(reg[k], r)
    return reg
}

func (reg runnersReg) Get(k interface{}) []Runner {
    return reg[registryKey(k)]
}

// registry of types
type typesReg map[interface{}][]Type
func (reg typesReg) Register(k interface{}, t Type) typesReg {
    registerGob(t, t.Data(0))
    k = registryKey(k)
    reg[k] = append(reg[k], t)
    return reg
}

func (reg typesReg) Get(k interface{}) []Type {
    return reg[registryKey(k)]
}

// Converts a key interface to a registry key according to the convention
// mentioned in the Registries doc. if the key is a struct, it's first converted
// into a string by reflecting its full type name and path
func registryKey(k interface{}) interface{} {
    // optimization - if it's just a string. Avoid the overhead of reflection.
    s, ok := k.(string)
    if ok {
        return s
    }

    t := reflect.TypeOf(k)
    if t.Kind() == reflect.Struct {
        return t.PkgPath() + "." + t.Name()
    }

    return k
}
