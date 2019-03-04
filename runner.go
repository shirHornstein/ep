package ep

import (
	"context"
)

var _ = registerGob(&passThrough{}, &pick{}, &tail{})

// UnknownSize is used when size cannot be estimated
const UnknownSize = -1

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
	//
	// NOTE: By convention, empty Datasets should never be sent to `out`
	// stream. The behavior of other Runners in case they receive empty
	// Datasets to `inp` stream is undefined.
	Run(ctx context.Context, inp, out chan Dataset) error

	// Returns the constant list of data types that are produced by this Runner.
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
	Returns() []Type
}

// RunnerArgs is a Runner that also exposes a list of argument types that it
// must accept as input.
type RunnerArgs interface {
	Runner // it's a Runner

	// Args returns the list of types that the runner must accept as input
	Args() []Type
}

// RunnerPlan is a Runner that also acts as a Runner constructor. This is useful
// for cases when the Runner needs to be somehow configured, or even replaced
// altogether based on input arguments
type RunnerPlan interface {
	Runner // it's a Runner

	// Plan allows the Runner to plan itself given an arbitrary argument. The
	// argument is context-dependent: it can be an AST node, or a composite
	// object containing multiple properties.
	Plan(ctx context.Context, arg interface{}) (Runner, error)
}

// RunnerExec is a Runner that also supports command execution.
type RunnerExec interface {
	Runner // it's a Runner

	// Exec executes a command and returns last inserted id, a number of rows
	// affected by this command, and an error.
	Exec(context.Context) (lastID int64, rowsAffected int64, err error)
}

// FilterRunner is a Runner that also exposes the ability to choose which
// results to return, and which are irrelevant and can be replaced with dummy
// placeholder
type FilterRunner interface {
	Runner // it's a Runner

	// Filter modifies internal Runner to return only the columns that their
	// corresponding 'keep' values are true.
	// length of 'keep' should be same as internal Runner's return types
	Filter(keep []bool)
}

// ScopesRunner is a Runner that also exposes the ability
// to get all scopes involved
type ScopesRunner interface {
	Runner // it's a Runner

	// Scopes returns all internal runners scopes
	Scopes() StringsSet
}

// PushRunner is a Runner that also exposes the ability
// to push another runner into the internal runner
type PushRunner interface {
	ScopesRunner // it's a Runner

	// Push tries to push a given runner into internal runner and returns true if succeeded
	Push(toPush ScopesRunner) bool
}

// ApproxSizer is a Runner that can roughly predict the size of its output
type ApproxSizer interface {
	Runner

	// ApproxSize returns a roughly estimated size of the output produced by this Runner
	ApproxSize() int
}

// Run runs given runner and takes care of channels management involved in runner execution
// safe to use only if caller created the out channel
func Run(ctx context.Context, r Runner, inp, out chan Dataset, cancel context.CancelFunc, err *error) {
	// drain inp in case there are left overs in the channel.
	// usually this will be a no-op, unless runner has exited early due to an
	// error or some other logic (irrelevant inp, etc.). in such cases draining
	// allows preceding runner to be canceled
	defer drain(inp)
	defer close(out)
	*err = r.Run(ctx, inp, out)
	if *err != nil && cancel != nil {
		setError(ctx, *err)
		cancel()
	}
}

// PassThrough returns a runner that lets all of its input through as-is
func PassThrough(types ...Type) Runner {
	if len(types) == 0 {
		return passThroughSingleton
	}
	return &passThrough{ReturnTypes: types}
}

// PassThroughWithScopes returns a runner that lets all of its input through as-is
func PassThroughWithScopes(scopes StringsSet, types ...Type) Runner {
	if len(types) == 0 {
		panic("scopes without types are not allowed")
	}
	return &passThrough{types, scopes}
}

var passThroughSingleton = &passThrough{}

type passThrough struct {
	ReturnTypes []Type
	scopes      StringsSet
}

func (r *passThrough) Scopes() StringsSet {
	return r.scopes
}

func (*passThrough) Args() []Type { return []Type{Wildcard} }
func (r *passThrough) Returns() []Type {
	if len(r.ReturnTypes) == 0 {
		return []Type{Wildcard}
	}
	return r.ReturnTypes
}
func (*passThrough) Run(_ context.Context, inp, out chan Dataset) error {
	for data := range inp {
		out <- data
	}
	return nil
}
func (r *passThrough) run(data Dataset) (Dataset, error) {
	return data, nil
}
func (r *passThrough) BatchFunction() BatchFunction {
	return r.run
}

// Pick returns a new runner similar to PassThrough except that it picks and
// returns just the data at the provided indices
func Pick(indices ...int) Runner { return &pick{indices} }

type pick struct{ Indices []int }

func (r *pick) Returns() []Type {
	types := make([]Type, len(r.Indices))
	for i, idx := range r.Indices {
		types[i] = Wildcard.At(idx)
	}

	return types
}

func (r *pick) Run(_ context.Context, inp, out chan Dataset) error {
	for data := range inp {
		var result Dataset
		if len(r.Indices) == 0 {
			result = NewDataset(dummy.Data(data.Len()))
		} else {
			res := make([]Data, len(r.Indices))
			for i, idx := range r.Indices {
				res[i] = data.At(idx)
			}
			result = NewDataset(res...)
		}
		out <- result
	}
	return nil
}

// helper function to drain inp/out channel
func drain(c chan Dataset) {
	for range c {
	}
}

// Tail returns a runner that split data and returns only last tailWidth columns
func Tail(returnTypes []Type) Runner {
	return &tail{returnTypes}
}

type tail struct {
	Types []Type
}

func (r *tail) Returns() []Type { return r.Types }

func (r *tail) Run(_ context.Context, inp, out chan Dataset) error {
	tailWidth := len(r.Types)
	for data := range inp {
		_, secondDataset := data.Split(tailWidth)
		out <- secondDataset
	}
	return nil
}
