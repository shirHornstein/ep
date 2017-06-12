package ep

// Project returns a horizontal composite projection runner that dispatches
// its input to all of the internal streams, and joins the result into a single
// dataset to return.
func Project(runners ...Runner) Runner {
    return project(runners)
}

type project []Runner

// Run dispatches the same input to all inner streams, and then collects and
// joins their results into a single dataset output
func (streams project) Run(ctx Ctx, inp, out chan Dataset) (err error) {
    if len(streams) == 0 {
        // NB Do we really want this? Shouldn't we just fail for an empty
        // projection? At the time of writing it only happened due to bugs in
        // the code - not due to any realistic use-case
        for range inp {} // drain it to unblock sends to inp
        return nil
    }

    // Run all inner streams
    inputs := make([]chan Dataset, len(streams))
    outputs := make([]chan Dataset, len(streams))
    for i := range streams {
        inputs[i] = make(chan Dataset)
        outputs[i] = make(chan Dataset)

        go func(i int) {
            defer close(outputs[i])
            err1 := streams[i].Run(ctx, inputs[i], outputs[i])
            if err1 != nil && err == nil {
                err = err1
            }
        }(i)
    }

    // Dispatch input to all inner streams
    go func() {
        for data := range inp {
            for _, s := range inputs {
                s <- data
            }
        }

        // close all inner streams
        for _, s := range inputs {
            close(s)
        }
    }()

    // collect the output from all inner streams, in order.
    //
    // TODO this assumes uniformity of all composed functions such that the
    // datasets are of equal lengths. Of course this assumption is wrong, and
    // the code should be adapted to support varying outputs of the individual
    // functions
    done := false
    for !done {
        result := Dataset{}
        for _, s := range outputs {
            res, ok := <- s
            done = done || !ok
            result = append(result, res...)
        }

        if result.Width() > 0 && result.Len() > 0 {
            out <- result
        }
    }

    return err
}
