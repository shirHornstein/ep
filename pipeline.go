package ep

import (
	"context"
	"sync"
)

var _ = registerGob(pipeline([]Runner{}))

// Pipeline returns a vertical composite pipeline runner where the output of
// any one stream is passed as input to the next
func Pipeline(runners ...Runner) Runner {
	if len(runners) == 0 {
		panic("at least 1 runner is required for pipelining")
	}

	// flatten nested pipelines. note we should examine only first level, as any
	// pre-created pipeline was already flatten during its creation
	var flat pipeline
	for _, r := range runners {
		p, isPipe := r.(pipeline)
		if isPipe {
			flat = append(flat, p...)
		} else {
			flat = append(flat, r)
		}
	}

	// filter out passThrough runners as they don't affect the pipe
	filtered := flat[:0]
	for _, r := range flat {
		if r != passThroughSingleton {
			filtered = append(filtered, r)
		}
	}

	if len(filtered) == 0 {
		// all runners were filtered, pipe should simply return its output as is
		return PassThrough()
	} else if len(filtered) == 1 {
		return filtered[0] // only one runner left, no need for a pipeline
	}
	return filtered
}

type pipeline []Runner

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	// choose first error out from all errors
	errs := make([]error, len(rs))
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		for _, e := range errs {
			if e != nil && e != context.Canceled {
				err = e
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)

	// run all (except the very last one) internal runners, piping the output
	// from each runner to the next.
	lastIndex := len(rs) - 1
	for i := 0; i < lastIndex; i++ {
		// middle chan is the output from the current runner and the input to
		// the next. We need to wait until this channel is closed before this
		// Run() function returns to avoid leaking go routines. This is achieved
		// by draining it. Only when the middle channel is closed we can know for
		// certain that the go routine has exited. Usually this will be a no-op,
		// but other times there might still be left overs in the channel. This
		// can happen if the top runner has exited early due to an error or some
		// other logic (LIMIT).
		middle := make(chan Dataset)
		defer drain(middle)

		wg.Add(1)
		go func(i int, inp, middle chan Dataset) {
			defer wg.Done()
			defer close(middle)
			errs[i] = rs[i].Run(ctx, inp, middle)
			if errs[i] != nil {
				cancel()
			}
		}(i, inp, middle)

		// input to the next channel is the output from the current one.
		inp = middle
	}

	// cancel all runners when we're done - just in case some are still running. This
	// might happen if the top of the pipeline ends before the bottom of the pipeline.
	defer cancel()

	// block until last runner completion
	errs[lastIndex] = rs[lastIndex].Run(ctx, inp, out)
	return
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

// Args returns the arguments expected by the first runner in this pipeline.
// If that runner is an instance of RunnerArgs, its Args() result will be returned.
// Otherwise, Wildcard type is returned.
func (rs pipeline) Args() []Type {
	runnerArgs, ok := rs[0].(RunnerArgs)
	if ok {
		return runnerArgs.Args()
	}
	return []Type{Wildcard}
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

func (rs pipeline) Filter(keep []bool) {
	lastIdx := len(rs) - 1
	last := rs[lastIdx]
	// pipeline contains at least 2 runners.
	// if last is exchange, filter its input (i.e. one runner before last)
	if _, isExchanger := last.(*exchange); isExchanger {
		last = rs[lastIdx-1]
	}
	if f, isFilterable := last.(FilterRunner); isFilterable {
		f.Filter(keep)
	}
}
