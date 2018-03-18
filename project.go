package ep

import (
	"context"
	"fmt"
	"sync"
)

var _ = registerGob(project([]Runner{}))
var errProjectState = fmt.Errorf("mismatched runners state")

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

// Returns a concatenation of all runners' return types
func (rs project) Returns() []Type {
	types := []Type{}
	for _, r := range rs {
		types = append(types, r.Returns()...)
	}
	return types
}

// Run dispatches the same input to all inner runners, then collects and
// joins their results into a single dataset output
func (rs project) Run(origCtx context.Context, inp, out chan Dataset) (err error) {
	// set up the left and right channels
	inps := make([]chan Dataset, len(rs))
	outs := make([]chan Dataset, len(rs))
	errs := make([]error, len(rs))
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		// choose first error out from all errors, that isn't project internal error
		for _, errI := range errs {
			if errI != nil && errI.Error() != errProjectState.Error() {
				err = errI
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(origCtx)

	// run all runners in go-routines
	for i := range rs {
		inps[i] = make(chan Dataset)
		outs[i] = make(chan Dataset)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = rs[idx].Run(ctx, inps[idx], outs[idx])
			close(outs[idx])
			// in case of error - drain inps[idx] to allow project keep
			// duplicating data
			if errs[idx] != nil {
				cancel()
			}
			go func() {
				for range inps[idx] {
				}
			}()
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := range rs {
			defer close(inps[i])
		}

		for {
			select {
			case <-origCtx.Done():
				cancel()
				return
			case <-ctx.Done():
				return
			case data, ok := <-inp:
				if !ok {
					return
				}
				// dispatch (duplicate) input to all runners
				for i := range rs {
					inps[i] <- data
				}
			}
		}
	}()

	// cancel all runners when we're done - just in case few still running
	// NOTE: cancel must be first defer to be called, to allow internal runners to finish
	defer func() {
		if err != nil {
			cancel()
			for i := range rs {
				// drain all runners' output to allow them catch cancellation
				go func(i int) {
					for range outs[i] {
					}
				}(i)
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
				return errProjectState
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
