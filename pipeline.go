package ep

import (
	"context"
)

var _ = registerGob(pipeline([]Runner{}))

// Pipeline returns a vertical composite pipeline runner where the output of
// any one stream is passed as input to the next
func Pipeline(runners ...Runner) Runner {
	if len(runners) == 0 {
		panic("at least 1 runner is required for pipelining")
	}

	var ans pipeline
	for _, r := range runners {
		// avoid nested pipelines by adding only real runners to returned ans.
		// note we should examine only first level, as any pre-created pipeline
		// was already flatten during its creation
		if p, isPipe := r.(pipeline); isPipe {
			for _, r2 := range p {
				// skip passThrough runners, as they don't affect the pipe
				if _, isPassThrough := r2.(*passthrough); !isPassThrough {
					ans = append(ans, r2)
				}
			}
		} else if _, isPassThrough := r.(*passthrough); !isPassThrough {
			ans = append(ans, r)
		}
	}

	// if all runners were filtered - pipe should simply return its output as is
	if len(ans) == 0 {
		return PassThrough()
	} else if len(ans) == 1 {
		return ans[0]
	}
	return ans
}

type pipeline []Runner

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	// choose first error out from all errors
	errs := make([]error, len(rs))
	defer func() {
		for _, errI := range errs {
			if errI != nil {
				err = errI
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)

	// run all of the internal runners (all except the very last one), piping
	// the output from each runner to the next.
	for i := 0; i < len(rs) - 1; i++ {

		// middle chan is the output from the current runner and the input to
		// the next. Drain it until the go-routine has exited. Usually this will
		// be a no-op, but other times there might still be left overs in the
		// channel. This can happen if the top runner has exited early due to an
		// error or some other logic (LIMIT). This prevents leaking go-routines.
		middle := make(chan Dataset)
		defer func() {
			for range middle {
			}
		}()

		go func(i int, inp, middle chan Dataset) {
			defer close(middle)
			errs[i] = rs[i].Run(ctx, inp, middle)
		}(i, inp, middle)

		// input to the next channel is the output from the current one.
		inp = middle
	}

	// cancel the all of the runners when we're done - just in case some are
	// still running. This might happen if the top of the pipeline ends before
	// the bottom of the pipeline.
	defer cancel()

	// run the last runner to completion.
	return rs[len(rs) - 1].Run(ctx, inp, out)
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

			if w.CutFromTail > 0 {
				prev = prev[:len(prev)-w.CutFromTail]
			}
			if w.Idx != nil {
				// wildcard for a specific column in the input
				prev = prev[*w.Idx : *w.Idx+1]
			}

			res = append(res[:i], append(prev, res[i+1:]...)...)
		}
	}
	return res
}
