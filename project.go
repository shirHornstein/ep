package ep

import (
    "context"
)

// Project returns a horizontal composite projection runner that dispatches
// its input to all of the internal runners, and joins the result into a single
// dataset to return.
func Project(runners ...Runner) Runner {
    return project(runners)
}

type project []Runner

// Run dispatches the same input to all inner runners, and then collects and
// joins their results into a single dataset output
func (rs project) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    if len(rs) == 0 {
        // NB Do we really want this? Shouldn't we just fail for an empty
        // projection? At the time of writing it only happened due to bugs in
        // the code - not due to any realistic use-case
        for range inp {} // drain it to unblock sends to inp
        return nil
    }

    // Run all inner runners
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

    // Dispatch input to all inner runners
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

    // collect the output from all inner runners, in order.
    //
    // TODO this assumes uniformity of all composed functions such that the
    // datasets are of equal lengths. Of course this assumption is wrong, and
    // the code should be adapted to support varying outputs of the individual
    // functions
    done := false
    for !done {
        result := dataset{}
        for _, s := range outputs {
            res, ok := <- s
            done = done || !ok
            for i := 0; i < res.Width(); i++ {
                result = append(result, res.At(i))
            }
        }

        if result.Width() > 0 && result.Len() > 0 {
            out <- result
        }
    }

    return err
}

// Returns a concatenation of all of the inner runners
func (rs project) Returns() []Type {
    rets := []Type{}
    for _, r := range rs {
        rets = append(rets, r.Returns()...)
    }
    return rets
}
