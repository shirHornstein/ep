package ep

import (
	"context"
	"fmt"
	"reflect"
)

// Runners registry. See Registries in the main doc.
var Runners = make(runnersReg)

// Types registry. See Registries in the main doc.
var Types = make(typesReg)

// Plan a new Runner marked by an arbitrary argument that must've been
// preregistered using the `Runners.Register()` function. if the arg is a
// struct, it's first converted into a string by reflecting its full type name
// and path. See Planning & Registries in the main doc.
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
		err = &errUnregistered{arg}
	}

	return nil, err
}

// registry of runners
// by convention - lowercase is function names, uppercase is SQL constructs
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

// All returns all registered types without duplications. useful for
// tests and code generation
func (reg typesReg) All() []Type {
	typesSet := make(map[Type]bool)
	for _, list := range reg {
		for _, t := range list {
			typesSet[t] = true
		}
	}

	types := make([]Type, 0, len(typesSet))
	for t := range typesSet {
		types = append(types, t)
	}
	return types
}

// Converts a key interface to a registry key according to the convention
// mentioned in the Registries doc. if the key is a struct, it's first converted
// into a string by reflecting its full type name and path
func registryKey(k interface{}) interface{} {
	// optimization - if it's just a string. Avoid the overhead of reflection
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

// error indicating that an unregistered argument was sent to Plan
type errUnregistered struct{ Arg interface{} }

func (err *errUnregistered) UnregisteredArg() interface{} { return err.Arg }
func (err *errUnregistered) Error() string {
	return fmt.Sprintf("Unregistered %s", reflect.TypeOf(err.Arg))
}
