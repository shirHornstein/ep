package ep

import (
    "context"
)

// Runner represents objects that can receive a stream of input datasets,
// manipulate them in some way (filter, mapping, reduction, expansion, etc.) and
// and produce a new stream of the formatted values.
// NOTE: Some Runners will run concurrently, this it's important to not modify
// the input in-place. Instead, copy/create a new dataset and use that
type Runner interface {

    // Run the manipulation code. Receive datasets from the `inp` stream, cast
    // and modify them as needed (no in-place), and send the results to the
    // `out` stream. Return when the `inp` is closed.
    //
    // NOTE: This function will run concurrently with other runners, so it needs
    // to be thread-safe. Ensure to clean up any created resources, including
    // goroutines, files, connections, etc. Closing the provided `inp` and `out`
    // channels is unnecessary as it's handled by the code that triggered this
    // Run() function. But, of course, if this Runner uses other Runners and
    // creates its own input and output channels, it should make sure to close
    // them as needed
    //
    // NOTE: For long-running producing runners (runners that given a small
    // input can produce un-proportionally large output, like scans, reading
    // from file, etc.), you should receive from the context's Done() channel to
    // know to break early in case of cancellation or an error to avoid doing
    // extra work. For most Runners, this is not as critical, because their
    // input will just close early.
    Run(ctx context.Context, inp, out chan Dataset) error

    // Types returns the constant list of data types that are produced by this
    // Runner.
    //
    // NOTE: Violation of meeting these defined types (either by producing
    // mismatching number of Data objects within the produced Datasets, or by
    // returning incorrect types) may result in a panic or worse - incorrect
    // results
    //
    // NOTE: If you need to annotate the returned data with names for
    // referencing later, use the `As()` helper function
    //
    // NOTE: In some cases you may not know the returned types ahead of time,
    // because it's somehow depends on the input types. For such cases, use the
    // Wildcard type.
    Types() []Type
}

// NewPassThrough returns a new runner that lets all of its input through as-is
func NewPassThrough() Runner {
    return passthrough{}
}

type passthrough struct {}

func (*passThrough) Run(_ Ctx, inp, out chan Dataset) (err error) {
    for data := range inp {
        out <- data
    }
}
