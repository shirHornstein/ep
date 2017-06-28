package ep

import (
    "fmt"
    "context"
)

var _ = registerGob(&union{})

// Union returns a new composite Runner that dispatches its inputs to all of
// its internal runners and collects their output into a single unified stream
// of datasets. It is required the all of the individual runners returns the
// same data types
func Union(runners ...Runner) (Runner, error) {
    if len(runners) == 0 {
        err := fmt.Errorf("at least 1 runner is required for union")
        return nil, err
    } else if len(runners) == 1 {
        return runners[0], nil
    }

    // determine the return types - skipping NULLS as they don't expose any
    // information about the actual data types.

    // ensure that the return types are compatible
    types := runners[0].Returns()
    for _, r := range runners {
        have := r.Returns()
        if len(have) != len(types) {
            return nil, fmt.Errorf("mismatch number of columns: %v and %v", types, have)
        }

        for i, t := range have {

            // choose the first column type that isn't a null
            if types[i] == Null {
                types[i] = have[i]
            } else if t != Null && t.Name() != types[i].Name() {
                return nil, fmt.Errorf("type mismatch %v and %v", types, have)
            }
        }
    }

    return &union{types, runners}, nil
}

type union struct {
    Types []Type
    Runners []Runner
}

// see Runner. Assumes all runners has the same return types.
func (r *union) Returns() []Type {
    return r.Types
}

func (r *union) Run(ctx context.Context, inp, out chan Dataset) (err error) {

    // start all inner runners
    inputs := make([]chan Dataset, len(r.Runners))
    outputs := make([]chan Dataset, len(r.Runners))
    for i := range r.Runners {
        inputs[i] = make(chan Dataset)
        outputs[i] = make(chan Dataset)

        go func(i int) {
            defer close(outputs[i])
            err1 := r.Runners[i].Run(ctx, inputs[i], outputs[i])
            if err1 != nil && err == nil {
                err = err1
            }
        }(i)
    }

    // fork the input to all inner runners
    go func() {
        for data := range inp {
            for _, s := range inputs {
                s <- data
            }
        }

        // close all inner runners
        for _, s := range inputs {
            close(s)
        }
    }()

    // collect and union all of the stream into a single output
    for _, s := range outputs {
        for data := range s {
            out <- data
        }
    }

    return err
}
