// Package eptest contains only tests utilities (without actual tests).
package eptest

import (
	"context"
	"github.com/panoplyio/ep"
)

// Run is helper function for tests, that runs given runner with given
// list of input datasets. Output is consumed up to completion, then returned
func Run(r ep.Runner, datasets ...ep.Dataset) (ep.Dataset, error) {
	return RunWithContext(context.Background(), r, datasets...)
}

// RunWithContext is helper function for tests, doing the same as Run
// with given context
func RunWithContext(ctx context.Context, r ep.Runner, datasets ...ep.Dataset) (res ep.Dataset, err error) {
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	go func() {
		err = r.Run(ctx, inp, out)
		close(out)
	}()

	go func() {
		for _, data := range datasets {
			inp <- data
		}
		close(inp)
	}()

	res = ep.NewDataset()
	for data := range out {
		res = res.Append(data).(ep.Dataset)
	}
	return res, err
}
