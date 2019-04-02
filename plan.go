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
func Plan(ctx context.Context, k interface{}) (Runner, error) {
	return PlanWithArgs(ctx, k, nil)
}

// PlanList plans list of items and returns an array of their runners
func PlanList(ctx context.Context, items []interface{}) ([]Runner, error) {
	// items can contain nil values (e.g. unary operators like ~,-,! will have one
	// nil operand). filter nil values as there is no need to plan them
	for i := 0; i < len(items); i++ {
		if items[i] == nil {
			items = append(items[:i], items[i+1:]...)
			i--
		}
	}

	if len(items) == 0 {
		return []Runner{Pick()}, nil // pick nothing
	}

	runners := make([]Runner, 0, len(items))
	for _, n := range items {
		r, err := Plan(ctx, n)
		if err != nil {
			return nil, err
		}

		if p, ok := r.(project); ok {
			runners = append(runners, p...)
		} else if p, ok := isComposeProject(r); ok {
			for _, cmp := range p {
				runners = append(runners, cmp.(Runner))
			}
		} else {
			runners = append(runners, r)
		}
	}
	return runners, nil
}

// PlanWithArgs is similar to Plan, except that it first filters the runners to
// only keep RunnerArgs instances that have the args provided.
func PlanWithArgs(ctx context.Context, k interface{}, args []Type) (Runner, error) {
	var err error
	var rs []Runner
	if args != nil {
		rs = Runners.GetWithArgs(k, args)
	} else {
		rs = Runners.Get(k)
	}

	for _, r := range rs {
		// check if the runner is plannable
		p, ok := r.(RunnerPlan)
		if !ok {
			// not a plannable Runner, return it as-is.
			return r, nil
		}

		// otherwise - let it plan itself
		r, err = p.Plan(ctx, k)
		if err == nil {
			return r, nil
		}
	}

	if err == nil {
		err = &errUnregistered{k}
	}

	return nil, err
}

// registry of runners
// by convention - lowercase is function names, uppercase is SQL constructs
type runnersReg map[interface{}][]Runner

// Register a key-runner pair to be globally accessible via the Get() function
// using the same key.
func (reg runnersReg) Register(k interface{}, r Runner) runnersReg {
	registerGob(k, r)
	k = registryKey(k)
	reg[k] = append(reg[k], r)
	return reg
}

// Get a list of Runners that were previously registered to the provided key
// via the Register() function.
func (reg runnersReg) Get(k interface{}) []Runner {
	return reg[registryKey(k)]
}

// GetWithArgs is similar to Get() except that it first filters the runners to
// only keep RunnerArgs instances that have the args provided.
func (reg runnersReg) GetWithArgs(k interface{}, args []Type) []Runner {
	rs := reg.Get(k)
	res := make([]Runner, 0, len(rs))
	for _, r := range rs {
		r, ok := r.(RunnerArgs)
		if ok && AreEqualTypes(r.Args(), args) {
			res = append(res, r)
		}
	}
	return res
}

// registry of types
type typesReg map[interface{}][]Type

// Register a key-type pair to be globally accessible via the Get() function
// using the same key.
func (reg typesReg) Register(k interface{}, t Type) typesReg {
	registerGob(t, t.Data(0))
	return reg.register(k, t)
}

func (reg typesReg) register(k interface{}, t Type) typesReg {
	k = registryKey(k)
	reg[k] = append(reg[k], t)
	return reg
}

// Get a list of Types that were previously registered to the provided key
// via the Register() function.
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
