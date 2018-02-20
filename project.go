package ep

import (
	"context"
	"sync"
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
	var wg sync.WaitGroup
	wg.Add(len(rs))

	// choose first error out from all errors
	defer func() {
		wg.Wait()
		for _, errI := range errs {
			if errI != nil {
				err = errI
				break
			}
		}
	}()

	// cancel all runners when we're done - just in case few still running
	ctx, cancel := context.WithCancel(ctx)

	defer func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-inp:
				if !ok { // inp was closed - no more input
					return
				}
			}
		}
	}()

	// run all runners in go-routines
	for i := range rs {
		inps[i] = make(chan Dataset)
		outs[i] = make(chan Dataset)
		go func(idx int) {
			defer func(idx int) {
				// in case project already done with error - drain others outs channel until empty
				close(outs[idx])
				if errs[idx] != nil {
					for i := range rs {
						defer func(i int) {
							for range outs[i] {
							}
						}(i)
					}
					cancel()
				}
			}(idx)
			errs[idx] = rs[idx].Run(ctx, inps[idx], outs[idx])
			wg.Done()
		}(i)
	}

	// dispatch (duplicate) input to all runners
	go func() {
		for i := range rs {
			defer func(i int) {
				close(inps[i])
			}(i)
		}
		for data := range inp {
			for i := range rs {
				inps[i] <- data
			}
		}
	}()

	// NOTE: cancel must be first defer to be called, to allow internal runners to finish
	defer cancel()

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

		if err == nil {
			out <- result
		}
	}
}
