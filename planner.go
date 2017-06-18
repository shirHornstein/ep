package ep

import (
    "fmt"
    "reflect"
    "context"
)

var runnersMap = make(map[string][]Runner)
var typesMap = make(map[string][]Type)

// Plan a new Runner marked by an arbitrary argument that must've been
// preregistered using the .Runners() function. See Planning above.
func Plan(ctx context.Context, arg interface{}) (Runner, error) {
    var err error
    for _, r := range Runners(arg) {

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

// Runners registers and returns a list of Runners, marked by an arbitrary key.
// Calling this function without any Runners will just return all of the
// registered runners. See Planning above.
func Runners(k interface{}, runners ...Runner) []Runner {
    s := keyToString(k)
    for _, r := range runners {
        runnersMap[s] = append(runnersMap[s], r)
    }
    return runnersMap[s]
}

// Types registers and returns a list of Types, marked by an arbitrary key.
// Calling this function without any types will just return all of the
// registered types for that key. See Planning above.
func Types(k interface{}, types ...Type) []Type {
    s := keyToString(k)
    for _, t := range types {
        typesMap[s] = append(typesMap[s], t)
    }

    return typesMap[s]
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
