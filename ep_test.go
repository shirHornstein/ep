package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"strings"
	"sync"
)

var _ = ep.Runners.
	Register("errRunner", &errRunner{}).
	Register("infinityRunner", &infinityRunner{}).
	Register("dataRunner", &dataRunner{}).
	Register("nodeAddr", &nodeAddr{}).
	Register("count", &count{}).
	Register("upper", &upper{}).
	Register("question", &question{})

// errRunner is a Runner that returns an error upon first input or inp closing
type errRunner struct {
	error
	// Name is unused field, defined to allow gob-ing errRunner between peers
	Name string
}

func NewErrRunner(e error) ep.Runner {
	return &errRunner{e, "err"}
}

func (*errRunner) Returns() []ep.Type { return []ep.Type{} }
func (r *errRunner) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	for range inp {
		return r.error
	}
	return r.error
}

// infinityRunner infinitely emits data until it's canceled
type infinityRunner struct {
	isRunningLock sync.Mutex
	// isRunning flag helps tests ensure that the go-routine didn't leak
	isRunning bool

	// Name is unused field, defined to allow gob-ing infinityRunner between peers
	Name string
}

func (*infinityRunner) Returns() []ep.Type { return []ep.Type{str} }
func (r *infinityRunner) IsRunning() bool {
	r.isRunningLock.Lock()
	defer r.isRunningLock.Unlock()
	return r.isRunning
}
func (r *infinityRunner) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	r.isRunningLock.Lock()
	r.isRunning = true
	r.isRunningLock.Unlock()

	defer func() {
		r.isRunningLock.Lock()
		r.isRunning = false
		r.isRunningLock.Unlock()
	}()

	// infinitely produce data, until canceled
	for { // TODO infinity
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-inp:
			if !ok {
				return nil
			}
			out <- ep.NewDataset(strs{"data"})
		}
	}
}

type dataRunner struct {
	ep.Dataset
	// ThrowOnData is a condition for throwing error. in case the last column
	// contains exactly this string in first row - fail with error
	ThrowOnData string
}

func (r *dataRunner) Returns() []ep.Type {
	var types []ep.Type
	for i := 0; i < r.Dataset.Len(); i++ {
		types = append(types, r.Dataset.At(i).Type())
	}
	return types
}
func (r *dataRunner) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		if r.ThrowOnData == data.At(data.Width() - 1).Strings()[0] {
			return fmt.Errorf("error %s", r.ThrowOnData)
		}
	}
	out <- r.Dataset
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
		if data.At(0).Type() == ep.Null {
			out <- data
			continue
		}

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
		if data.At(0).Type() == ep.Null {
			out <- data
			continue
		}

		res := make(strs, data.Len())
		for i, v := range data.At(0).(strs) {
			res[i] = "is " + v + "?"
		}
		out <- ep.NewDataset(res)
	}
	return nil
}

// runner that accepts strings, and breaks up the characters in the string to
// one character per row.
type breakChars struct{}

func (*breakChars) Returns() []ep.Type { return []ep.Type{str} }
func (*breakChars) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		for _, s := range data.At(0).(strs) {
			res := make(strs, len(s))
			for i, c := range s { // iterate on the characters
				res[i] = string(c)
			}
			out <- ep.NewDataset(res)
		}
	}
	return nil
}
