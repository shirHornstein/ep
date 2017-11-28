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
	} else if len(runners) == 1 {
		return runners[0]
	}
	return pipeline(runners)
}

type pipeline []Runner

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	return rs.runOne(ctx, len(rs)-1, inp, out)
}

func (rs pipeline) runOne(ctx context.Context, i int, inp, out chan Dataset) (err error) {
	if i == 0 {
		return rs[i].Run(ctx, inp, out)
	}

	// choose the error out from the From and To errors.
	var err1 error
	defer func() {
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	// middle chan is the output from the From runner and the input to the To
	// Runner. Drain it until the go-routine has exited. Usually this will be a
	// no-op, but other times there might still be left overs in the channel.
	// This can happen if the top (To) runner has exited early due to an error
	// or some other logic (LIMIT?). This prevents leaking go-routines
	middle := make(chan Dataset)
	defer func() {
		for range middle {
		}
	}()

	// cancel the From runner when we're done - just in case it's still running.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// start the From runner, writing data into the middle chan
	go func() {
		defer close(middle)
		err1 = rs.runOne(ctx, i-1, inp, middle)
	}()

	return rs[i].Run(ctx, middle, out)
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
	if j == 0 {
		return rs[j].Returns()
	}

	res := rs[j].Returns()

	// check for wildcards, and replace as needed. Walk backwards to allow
	// adding types in-place without messing the iteration
	for i := len(res) - 1; i >= 0; i-- {
		w, isWildcard := res[i].(*wildcardType)

		if isWildcard {
			// wildcard found - replace it with the types from the previous
			// runner (which might also contain Wildcards)
			prev := rs.returnsOne(j - 1)

			if w.Idx != nil {
				// wildcard for a specific column in the input
				prev = prev[*w.Idx : *w.Idx+1]
			}

			res = append(res[:i], append(prev, res[i+1:]...)...)
		}
	}

	return res
}
