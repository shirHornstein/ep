package ep

import (
    "fmt"
    "reflect"
    "context"
)

// Planner implements Registry and RunnerPlan, and uses the registered Runners
// for planning based on their configured key argument.
//
// Plan creates a Runner for the provided arg. It uses the list of
// registered Runners (see .Runner()) to lookup the relevant Runner. If the
// resolved Runner implements a similarily typed Plan() function, it's first
// called with the same arguments to allow the Runner to plan itself.
//
// A key can be bound to multiple runners, in which case the Runners will be
// resolved as a list of middleware - callling each one's Plan method, in
// turn until one of them doesn't return and error. This means that, upon
// error, Plan will fallthrough to the next Runner. This allows an
// opportunistic design where several runners bind to the same node, each
// planning it differently - if they can.
// Plan(ctx context.Context, k interface{}) (Runner, error)
var Planner = newPlanner()

// Registry is a utility object that stores Types and Runners by an arbitrary
// key argument. Its purpose is to destignate a lookup key used for finding the
// relevant Runner or Type to be used. You can think of it like a key-value
// store with the caveat that - if it's not a string - the key is first
// reflected to the type name of the object, which is then used instead. This
// means that we can register and use key objects, instead of simple strings.
//
// For example, if we're using SQL to construct Runners, we can bind an AST
// node:
//
//      Planner.Runner(ast.SelectStmt{}, &SelectRunner{})
//      Planner.Plan(nil, ast.SelectStmt{}) // Returns &SelectRunner{}
//
type Registry interface {

    // Type registers and returns a list of Types, marked by a key. Calling this
    // function without any types will just return all of the registered types
    Types(k interface{}, types ...Type) []Type

    // Runner registers and returns a list of Runners, marked by a key. Calling
    // this function without any Runners will just return all of the registered
    // runners.
    Runners(k interface{}, runners ...Runner) []Runner
}


type planner struct {
    runners map[string][]Runner
    types map[string][]Type
}

func newPlanner() *planner {
    return &planner{
        make(map[string][]Runner),
        make(map[string][]Type),
    }
}

func (p *planner) Plan(ctx context.Context, k interface{}) (Runner, error) {
    var err error
    for _, r := range p.Runners(k) {

        // check if the runner is plannable
        p, ok := r.(interface {
            Plan(context.Context, interface{}) (Runner, error)
        })

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
        err = fmt.Errorf("Unsupported")
    }

    return nil, err
}

func (p *planner) Types(k interface{}, types ...Type) []Type {
    s := keyToString(k)
    for _, t := range types {
        p.types[s] = append(p.types[s], t)
    }

    return p.types[s]
}

func (p *planner) Runners(k interface{}, runners ...Runner) []Runner {
    s := keyToString(k)
    for _, r := range runners {
        p.runners[s] = append(p.runners[s], r)
    }
    return p.runners[s]
}

func keyToString(k interface{}) string {
    s, ok := k.(string)
    if ok {
        return s
    }

    t := reflect.TypeOf(k)
    return t.PkgPath() + "." + t.Name()
}
