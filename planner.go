package ep

import (
    "fmt"
    "reflect"
    "context"
)


func Plan(ctx context.Context, arg interface{}) (Runner, error) {
    return Planner.Plan(ctx, arg)
}

func Types(k interface{}, types ...Type) []Type {
    return Planner.Types(k, types...)
}

func Runners(k interface{}, runners ...Runner) []Runner {
    return Planner.Runners(k, runners...)
}


// Planner implements Registry and RunnerPlan, and uses the registered Runners
// for planning based on their configured key argument. Its Plan() function
// creates a Runner for the provided arg. It uses the list of
// registered Runners (see .Runner()) to lookup the relevant Runner. If the
// resolved Runner implements RunnerPlan, it's first called with the same
// arguments to allow the Runner to plan itself.
//
// A key can be bound to multiple runners, in which case the Runners will be
// resolved as a list of middlewares - callling each Plan() method in turn until
// one of them doesn't return and error. This means that, upon error, Plan will
// fallthrough to the next Runner. This allows an opportunistic design where
// several runners bind to the same node, each planning it differently - if they
// can.
//
// It cannot be directly used as a Runner
var Planner = newPlanner() // Registry & RunnerPlan

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

// convert an arbitrary key interface to a string - if it's an object, use the
// object's unique path and name instead.
func keyToString(k interface{}) string {
    s, ok := k.(string)
    if ok {
        return s
    }

    t := reflect.TypeOf(k)
    return t.PkgPath() + "." + t.Name()
}
