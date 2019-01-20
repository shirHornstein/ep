package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"strings"
	"sync"
	"time"
)

var _ = ep.Runners.
	Register("waitForCancel", &waitForCancel{}).
	Register("fixedData", &fixedData{}).
	Register("dataRunner", &dataRunner{}).
	Register("nodeAddr", &nodeAddr{}).
	Register("count", &count{}).
	Register("upper", &upper{}).
	Register("question", &question{})

// waitForCancel infinitely emits data until it's canceled
type waitForCancel struct {
	isRunningLock sync.Mutex
	// isRunning flag helps tests ensure that the go-routine didn't leak
	isRunning bool

	// Name is unused field, defined to allow gob-ing infinityRunner between peers
	Name string
}

func (*waitForCancel) Returns() []ep.Type { return []ep.Type{str} }
func (r *waitForCancel) IsRunning() bool {
	r.isRunningLock.Lock()
	defer r.isRunningLock.Unlock()
	return r.isRunning
}
func (r *waitForCancel) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	ticker := time.NewTicker(100 * time.Millisecond)

	r.isRunningLock.Lock()
	r.isRunning = true
	r.isRunningLock.Unlock()

	defer func() {
		r.isRunningLock.Lock()
		r.isRunning = false
		r.isRunningLock.Unlock()
	}()

	go func() {
		for range inp {
		} // drain input, waitForCancel doesn't depend in input
	}()

	// infinitely produce data, until canceled
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			out <- ep.NewDataset(strs{"data"})
		}
	}
}

type fixedData struct {
	ep.Dataset
}

func (r *fixedData) Returns() []ep.Type {
	var types []ep.Type
	for i := 0; i < r.Dataset.Len(); i++ {
		types = append(types, r.Dataset.At(i).Type())
	}
	return types
}
func (r *fixedData) Run(ctx context.Context, _, out chan ep.Dataset) (err error) {
	out <- r.Dataset
	return nil
}

type dataRunner struct {
	ep.Dataset
	// ThrowOnData is a condition for throwing error. in case the last column
	// contains exactly this string in first row - fail with error
	ThrowOnData   string
	ThrowCanceled bool
}

func (r *dataRunner) Returns() []ep.Type {
	var types []ep.Type
	for i := 0; i < r.Dataset.Len(); i++ {
		types = append(types, r.Dataset.At(i).Type())
	}
	return types
}
func (r *dataRunner) Run(ctx context.Context, inp, out chan ep.Dataset) (err error) {
	if r.ThrowCanceled {
		err = context.Canceled
	} else {
		err = fmt.Errorf("error %s", r.ThrowOnData)
	}
	for data := range inp {
		if r.ThrowOnData == data.At(data.Width() - 1).Strings()[0] {
			return err
		}
		out <- r.Dataset
	}
	return nil
}

type nodeAddr struct{}

func (*nodeAddr) Returns() []ep.Type { return []ep.Type{ep.Wildcard, str} }
func (*nodeAddr) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	addr := ep.NodeAddress(ctx)
	for data := range inp {
		res := make(strs, data.Len())
		for i := range res {
			res[i] = addr
		}

		var outset []ep.Data
		for i := 0; i < data.Width(); i++ {
			outset = append(outset, data.At(i))
		}

		outset = append(outset, res)
		out <- ep.NewDataset(outset...)
	}
	return nil
}

type count struct{}

func (*count) Returns() []ep.Type { return []ep.Type{str} }
func (*count) Run(_ context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		out <- ep.NewDataset(strs{fmt.Sprintf("%d", data.Len())})
	}
	return nil
}

type upper struct{}

func (*upper) Returns() []ep.Type { return []ep.Type{ep.SetAlias(str, "upper")} }
func (*upper) Run(_ context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		res := make(strs, data.Len())
		for i, v := range data.At(0).(strs) {
			res[i] = strings.ToUpper(v)
		}
		out <- ep.NewDataset(res)
	}
	return nil
}

type question struct {
	// called flag helps tests ensure runner was/wasn't called
	called bool
}

func (*question) Returns() []ep.Type { return []ep.Type{ep.SetAlias(str, "question")} }
func (q *question) Run(_ context.Context, inp, out chan ep.Dataset) error {
	q.called = true
	for data := range inp {
		res := make(strs, data.Len())
		for i, v := range data.At(0).(strs) {
			res[i] = "is " + v + "?"
		}
		out <- ep.NewDataset(res)
	}
	return nil
}

type adder struct{}

func (*adder) BatchFunction() ep.BatchFunction {
	return func(data ep.Dataset) (ep.Dataset, error) {
		d0 := data.At(0).(integers)
		d1 := data.At(1).(integers)
		res := make(integers, data.Len())
		for i := range res {
			res[i] = d0[i] + d1[i]
		}
		return ep.NewDataset(res), nil
	}
}

type opposer struct{}

func (*opposer) BatchFunction() ep.BatchFunction {
	return func(data ep.Dataset) (ep.Dataset, error) {
		d0 := data.At(0).(integers)
		res := make(integers, data.Len())
		for i := range res {
			res[i] = -1 * d0[i]
		}
		return ep.NewDataset(res), nil
	}
}

type mul2 struct{}

func (*mul2) BatchFunction() ep.BatchFunction {
	return func(data ep.Dataset) (ep.Dataset, error) {
		d0 := data.At(0).(integers)
		res := make(integers, data.Len())
		for i := range res {
			res[i] = d0[i] * 2
		}
		return ep.NewDataset(res), nil
	}
}
