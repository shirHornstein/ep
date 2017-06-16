package ep

import (
    "context"
)

// Union returns a new composite Runner that dispatches its inputs to all of
// its internal runners and collects their output into a single unified stream
// of datasets. It is required the all of the individual runners returns the
// same data types
func Union(runners ...Runner) Runner {
    return union(runners)
}

type union []Runner

// see Runner. Assumes all runners has the same return types.
func (rs union) Returns() []Type {
    if rs == nil || len(rs) == 0 {
        return nil
    }

    return rs[0].Returns()
}

func (rs union) Run(ctx context.Context, inp, out chan Dataset) (err error) {

    // start all inner runners
    inputs := make([]chan Dataset, len(rs))
    outputs := make([]chan Dataset, len(rs))
    for i := range rs {
        inputs[i] = make(chan Dataset)
        outputs[i] = make(chan Dataset)

        go func(i int) {
            defer close(outputs[i])
            err1 := rs[i].Run(ctx, inputs[i], outputs[i])
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
