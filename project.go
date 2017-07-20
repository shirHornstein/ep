package ep

import (
	"context"
	"fmt"
)

var _ = registerGob(project([]Runner{}))
var errMismatch = fmt.Errorf("ep.Project: mismatched number of rows")

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

// Run dispatches the same input to all inner runners, and then collects and
// joins their results into a single dataset output
func (rs project) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	return rs.runOne(ctx, len(rs)-1, inp, out)
}

func (rs project) runOne(ctx context.Context, i int, inp, out chan Dataset) (err error) {
	if i == 0 {
		return rs[i].Run(ctx, inp, out)
	}

	// choose the error out from the Left and Right errors.
	var err1 error
	var err2 error
	defer func() {
		if err1 != nil {
			err = err1
		} else if err2 != nil {
			err = err2
		}
	}()

	// set up the left and right channels
	inpLeft := make(chan Dataset)
	left := make(chan Dataset)
	defer func() {
		for range left { // drain left until empty.
		}
	}()

	inpRight := make(chan Dataset)
	right := make(chan Dataset)
	defer func() {
		for range right { // drain right until empty.
		}
	}()

	// cancel the both runners when we're done - just in case it's still running
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// run the left and right runners in go-routines
	go func() {
		defer close(left)
		err1 = rs.runOne(ctx, i-1, inpLeft, left)
	}()

	go func() {
		defer close(right)
		err2 = rs[i].Run(ctx, inpRight, right)
	}()

	// dispatch (duplicate) input to both left and right runners
	go func() {
		defer close(inpLeft)
		defer close(inpRight)
		for data := range inp {
			inpLeft <- data
			inpRight <- data
		}
	}()

	// collect & join the output from the Left and Right runners, in order.
	for {
		result := []Data{}
		dataLeft, okLeft := <-left
		dataRight, okRight := <-right

		if okLeft != okRight {
			return errMismatch // one's closed, the other is open
		}

		if !okLeft {
			return // all done.
		}

		if dataLeft.Len() != dataRight.Len() {
			return errMismatch
		}

		// TODO: what if there's a mismatch in Len()? Error may be best to
		// identify runners that were incorrectly constructed?
		for i := 0; okLeft && i < dataLeft.Width(); i++ {
			result = append(result, dataLeft.At(i))
		}

		for i := 0; okRight && i < dataRight.Width(); i++ {
			result = append(result, dataRight.At(i))
		}

		out <- NewDataset(result...)
	}
}
