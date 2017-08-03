package ep

import (
	"context"
)

// TestRunner is helper function for tests, that runs given runner with given
// list of input datasets. Output is consumed up to completion, then returned
func TestRunner(r Runner, datasets ...Dataset) (Dataset, error) {
	var err error

	inp := make(chan Dataset, len(datasets))
	for _, data := range datasets {
		inp <- data
	}
	close(inp)

	out := make(chan Dataset)
	go func() {
		err = r.Run(context.Background(), inp, out)
		close(out)
	}()

	var res = NewDataset()
	for data := range out {
		res = res.Append(data).(Dataset)
	}

	return res, err
}
