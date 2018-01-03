package ep

import (
	"context"
)

var _ = registerGob(project([]Runner{}))

// Project returns a horizontal composite projection runner that dispatches
// its input to all of the internal runners, and joins the result into a single
// dataset to return. It is required that all runners produce Datasets of the
// same length.
func Project(runners ...Runner) Runner {
	if len(runners) == 0 {
		panic("at least 1 runner is required for projecting")
	} else if len(runners) == 1 {
		return runners[0]
	}

	return project(runners)
}

type project []Runner

// Returns a concatenation of the left and right return types
func (rs project) Returns() []Type {
	types := []Type{}
	for _, r := range rs {
		types = append(types, r.Returns()...)
	}
	return types
}

// Run dispatches the same input to all inner runners, then collects and
// joins their results into a single dataset output
func (rs project) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	// set up the left and right channels
	inps := make([]chan Dataset, len(rs))
	outs := make([]chan Dataset, len(rs))
	errs := make([]error, len(rs))

	// choose first error out from all errors
	defer func() {
		for _, errI := range errs {
			if errI != nil {
				err = errI
				break
			}
		}
	}()

	// cancel all runners when we're done - just in case few still running
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// run all runners in go-routines
	for i := range rs {
		inps[i] = make(chan Dataset)
		outs[i] = make(chan Dataset)
		// in case project already done with error - drain until empty
		defer func(idx int) {
			for range outs[idx] {
			}
		}(i)
		go func(idx int) {
			defer close(outs[idx])
			errs[idx] = rs[idx].Run(ctx, inps[idx], outs[idx])
		}(i)
	}

	// dispatch (duplicate) input to all runners
	go func() {
		for i := range rs {
			defer close(inps[i])
		}
		for data := range inp {
			for i := range rs {
				inps[i] <- data
			}
		}
	}()

	// collect & join the output from all runners, in order
	for {
		result := NewDataset()
		var allOpen bool
		for i := range rs {
			curr, open := <-outs[i]

			// verify all still open or all closed
			if i == 0 {
				allOpen = open // init allOpen according to first out channel
			} else if allOpen != open {
				cancel()
				return errMismatch
			}

			if open {
				result, err = result.Expand(curr)
				if err != nil {
					return err
				}
			}
		}

		if !allOpen {
			return // all done
		}

		out <- result
	}
}
