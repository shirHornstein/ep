package ep

// NewUnion returns a new composite Runner that dispatches its inputs to all of
// its internal runners and collects their output into a single unified stream
// of datasets. It is required the all of the individual runners returns the
// same data types
func NewUnion(runners ...Runner) Runner {
    return union(runners)
}

type union []Runner

func (streams union) Run(ctx Ctx, inp, out chan Dataset) (err error) {

    // start all inner streams
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

    // fork the input to all inner streams
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

    // collect and union all of the stream into a single output
    for _, s := range outputs {
        for data := range s {
            out <- data
        }
    }

    return err
}
