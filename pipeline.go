package ep

import (
	"context"
	"sync"
)

var _ = registerGob(pipeline([]Runner{}))
var contextCanceledErrorMessage = "context canceled"

// Pipeline returns a vertical composite pipeline runner where the output of
// any one stream is passed as input to the next
func Pipeline(runners ...Runner) Runner {
	if len(runners) == 0 {
		panic("at least 1 runner is required for pipelining")
	}

	// flatten nested pipelines. note we should examine only first level, as any
	// pre-created pipeline was already flatten during its creation
	flat := []Runner{}
	for _, r := range runners {
		p, isPipe := r.(pipeline)
		if isPipe {
			flat = append(flat, p...)
		} else {
			flat = append(flat, r)
		}
	}

	// filter out passthrough runners as they don't affect the pipe
	filtered := []Runner{}
	for _, r := range flat {
		_, isPassthrough := r.(*passthrough)
		if !isPassthrough {
			filtered = append(filtered, r)
		}
	}

	if len(filtered) == 0 {
		return PassThrough() // all non-passthrough runners were filtered
	} else if len(filtered) == 1 {
		return filtered[0] // only one runner left, no need for a pipeline
	}

	return pipeline(filtered)
}

type pipeline []Runner

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	// choose first error out from all errors
	errs := make([]error, len(rs))
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		for _, errI := range errs {
			if errI != nil && errI.Error() != contextCanceledErrorMessage {
				err = errI
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)

	// run all of the internal runners (all except the very last one), piping
	// the output from each runner to the next.
	for i := 0; i < len(rs)-1; i++ {

		// middle chan is the output from the current runner and the input to
		// the next. Drain it until the go-routine has exited. Usually this will
		// be a no-op, but other times there might still be left overs in the
		// channel. This can happen if the top runner has exited early due to an
		// error or some other logic (LIMIT). This prevents leaking go-routines.
		middle := make(chan Dataset)
		defer func(middle chan Dataset) {
			for range middle {
			}
		}(middle)

		wg.Add(1)
		go func(i int, inp, middle chan Dataset) {
			defer wg.Done()
			defer close(middle)
			errs[i] = rs[i].Run(ctx, inp, middle)
		}(i, inp, middle)

		// input to the next channel is the output from the current one.
		inp = middle
	}

	// cancel the all of the runners when we're done - just in case some are
	// still running. This might happen if the top of the pipeline ends before
	// the bottom of the pipeline.
	defer func() {
		defer wg.Done()
		cancel()
	}()

	wg.Add(1)
	// block run the last runner until completed
	return rs[len(rs)-1].Run(ctx, inp, out)
}

// The implementation isn't trivial because it has to account for Wildcard types
// which indicate that the actual types should be retrieved from the input, thus
// when a Wildcard is found in the To runner, this function will replace it with
// the entire return types of the From runner (which might be another pair -
// calling this function recursively).
// see Runner & Wildcard
func (rs pipeline) Returns() []Type {
	return rs.returnsOne(len(rs) - 1) // start with the last one.
}

func (rs pipeline) returnsOne(j int) []Type {
	res := rs[j].Returns()
	if j == 0 {
		return res
	}

	// check for wildcards, and replace as needed. Walk backwards to allow
	// adding types in-place without messing the iteration
	for i := len(res) - 1; i >= 0; i-- {
		if w, isWildcard := res[i].(*wildcardType); isWildcard {
			// wildcard found - replace it with the types from the previous
			// runner (which might also contain Wildcards)
			prev := rs.returnsOne(j - 1)
			prev = prev[:len(prev)-w.CutFromTail]
			if w.Idx != nil {
				// wildcard for a specific column in the input
				prev = prev[*w.Idx : *w.Idx+1]
			}

			res = append(res[:i], append(prev, res[i+1:]...)...)
		}
	}
	return res
}
