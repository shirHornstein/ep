package ep

import (
	"context"
)

// TestRunner is helper function for tests, that runs given runner with given
// list of input datasets. Output is consumed up to completion, then returned
func TestRunner(r Runner, datasets ...Dataset) (Dataset, error) {
	return TestRunnerWithContext(context.Background(), r, datasets...)
}

// TestRunnerWithContext is helper function for tests, doing the same as TestRunner
// with given context
func TestRunnerWithContext(ctx context.Context, r Runner, datasets ...Dataset) (Dataset, error) {
	var err error
	inp := make(chan Dataset, len(datasets))
	out := make(chan Dataset)
	go func() {
		err = r.Run(ctx, inp, out)
		close(out)
	}()

	for _, data := range datasets {
		inp <- data
	}
	close(inp)

	var res = NewDataset()
	for data := range out {
		res = res.Append(data).(Dataset)
	}

	return res, err
}
