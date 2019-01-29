package ep

import (
	"context"
	"sync"
)

var _ = registerGob(project([]Runner{}), &dummyRunner{})

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

// Placeholder returns a project with a placeholder of shift size length
func Placeholder(shift int, runners ...Runner) Runner {
	p := Project(runners...)
	dummies := make([]Runner, shift)
	for i := range dummies {
		dummies[i] = dummyRunnerSingleton
	}
	var placeholder project
	placeholder = append(dummies, p)
	return placeholder
}

type project []Runner

func (rs project) Push(toPush ScopesRunner) bool {
	for _, p := range rs {
		if p, ok := p.(PushRunner); ok {
			isPushed := p.Push(toPush)
			if isPushed {
				return true
			}
		}
	}
	return false
}

func (rs project) Scopes() map[string]struct{} {
	scopes := make(map[string]struct{})
	for _, r := range rs {
		if r, ok := r.(ScopesRunner); ok {
			runnersScope := r.Scopes()
			for s := range runnersScope {
				scopes[s] = struct{}{}
			}
		}
	}
	return scopes
}

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
func (rs project) Run(ctx context.Context, inp, out chan Dataset) (err error) {
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
			if err == nil && errI != nil {
				err = errI
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)

	// run all runners in go-routines
	for i := range rs {
		inps[i] = make(chan Dataset)
		outs[i] = make(chan Dataset)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			Run(ctx, rs[idx], inps[idx], outs[idx], cancel, &errs[idx])
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		defer func() {
			for i := range rs {
				close(inps[i])
			}
		}()

		for {
			select {
			case <-ctx.Done(): // listen to both new ctx and original ctx
				cancel()
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
		cancel()
		for i := range rs {
			// drain all runners' output to allow them catch cancellation
			go drain(outs[i])
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
				return nil
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

// dummyRunnerSingleton is a runner that does nothing and just returns immediately
var dummyRunnerSingleton = &dummyRunner{}

// variadicDummiesBatch is used for replacing unused columns
var variadicDummiesBatch = NewDataset(dummy.Data(-1))

type dummyRunner struct{}

func (*dummyRunner) Args() []Type    { return []Type{Wildcard} }
func (*dummyRunner) Returns() []Type { return []Type{dummy} }
func (*dummyRunner) Run(_ context.Context, inp, out chan Dataset) error {
	return nil
}
