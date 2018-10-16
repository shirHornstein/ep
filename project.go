package ep

import (
	"context"
	"fmt"
	"sync"
)

var _ = registerGob(project([]Runner{}), &dummyRunner{})

var errProjectState = fmt.Errorf("mismatched runners state")

// Project returns a horizontal composite projection runner that dispatches
// its input to all of the internal runners, and joins the result into a single
// dataset to return. It is required that all runners produce Datasets of the
// same length.
func Project(runners ...Runner) Runner {
	// flatten nested projects. note we should examine only first level, as any
	// pre-created project was already flatten during its creation
	var flat project
	for _, r := range runners {
		p, isProject := r.(project)
		if isProject {
			flat = append(flat, p...)
		} else {
			flat = append(flat, r)
		}
	}

	if len(flat) == 0 {
		panic("at least 1 runner is required for projecting")
	} else if len(flat) == 1 {
		return runners[0]
	}

	return flat
}

type project []Runner

// Returns a concatenation of all runners' return types
func (rs project) Returns() []Type {
	var types []Type
	for _, r := range rs {
		types = append(types, r.Returns()...)
	}
	return types
}

func (rs project) Filter(keep []bool) {
	currIdx := 0
	for i, r := range rs {
		returnLen := len(r.Returns())
		// simplest (and most common for project) case - r return single value
		if returnLen == 1 {
			if !keep[currIdx] {
				rs[i] = dummyRunnerSingleton
			}
		} else {
			if r, isFilterable := r.(FilterRunner); isFilterable {
				r.Filter(keep[currIdx : currIdx+returnLen])
			}
		}
		currIdx += returnLen
	}
}

// Run dispatches the same input to all inner runners, then collects and
// joins their results into a single dataset output
func (rs project) Run(origCtx context.Context, inp, out chan Dataset) (err error) {
	rs.useDummySingleton()
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
		allOpen, allDummies := true, true
		for i := range rs {
			curr, open := <-outs[i]

			if rs[i] == dummyRunnerSingleton {
				// dummy runners shouldn't affect state check
				curr, open = variadicDummiesBatch, allOpen
			} else if allDummies {
				allDummies = false
				// init allOpen according to first out channel of non dummy runner
				allOpen = open
			} else if allOpen != open { // verify all still open or all closed
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

		if allDummies {
			return
		}
	}
}

// useDummySingleton replaces all dummies with pre-defined singleton to allow addresses comparison
// instead of casting for each batch.
// required for distribute runner that creates new dummy instances instead of using singleton
func (rs project) useDummySingleton() {
	for i, r := range rs {
		if _, isDummy := r.(*dummyRunner); isDummy {
			rs[i] = dummyRunnerSingleton
		}
	}
}

// dummyRunnerSingleton is a runner that does nothing and just drain inp
var dummyRunnerSingleton = &dummyRunner{}

// variadicDummiesBatch is used for replacing unused columns
var variadicDummiesBatch = NewDataset(dummy.Data(-1))

type dummyRunner struct{}

func (*dummyRunner) Args() []Type    { return []Type{Wildcard} }
func (*dummyRunner) Returns() []Type { return []Type{dummy} }
func (*dummyRunner) Run(_ context.Context, inp, out chan Dataset) error {
	return nil
}
