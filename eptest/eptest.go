// Package eptest contains only tests utilities (without actual tests).
package eptest

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"net"
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
	go ep.Run(ctx, r, inp, out, nil, &err)

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

// RunDist is like Run, but first distributes the runner to n nodes
func RunDist(n int, r ep.Runner, datasets ...ep.Dataset) (ep.Dataset, error) {
	var dist ep.Distributer
	var addrs []string
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf(":%04d", 5550+i) // 5551, 5552, 5553, ...
		addrs = append(addrs, addr)

		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}

		dist = ep.NewDistributer(addr, ln)
		defer dist.Close()
	}

	r = ep.Pipeline(r, ep.Gather())
	r = dist.Distribute(r, addrs...)
	return Run(r, datasets...)
}

// Bench is like Run except that it doesn't accumulate its output in memory
func Bench(r ep.Runner, datasets ...ep.Dataset) (err error) {
	return BenchWithContext(context.Background(), r, datasets...)
}

// BenchWithContext is helper function for tests, doing the same as Bench
// with given context
func BenchWithContext(ctx context.Context, r ep.Runner, datasets ...ep.Dataset) (err error) {
	out := make(chan ep.Dataset)
	inp := make(chan ep.Dataset, len(datasets))
	for _, data := range datasets {
		inp <- data
	}
	close(inp)

	go func() {
		defer close(out)
		err = r.Run(ctx, inp, out)
	}()

	for range out {
	}

	return err
}
