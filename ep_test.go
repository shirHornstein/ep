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
	Register("question", &question{}).
	Register("localSort", &localSort{})

const batchSize = 3

// waitForCancel infinitely emits data until it's canceled
type waitForCancel struct {
	isRunningLock sync.Mutex
	// isRunning flag helps tests ensure that the go-routine didn't leak
	isRunning bool

	// Name is unused field, defined to allow gob-ing infinityRunner between peers
	Name string
}

func (*waitForCancel) Equals(other interface{}) bool {
	_, ok := other.(*waitForCancel)
	return ok
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

func (f *fixedData) Equals(other interface{}) bool {
	r, ok := other.(*fixedData)
	return ok && f.Dataset.Equal(r.Dataset)
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
	// ThrowOnData is a condition for throwing error. in case the last column
	// contains exactly this string in first row - fail with error
	ThrowOnData    string
	ThrowIgnorable bool
}

func (d *dataRunner) Equals(other interface{}) bool {
	r, ok := other.(*dataRunner)
	return ok && d.ThrowOnData == r.ThrowOnData && d.ThrowIgnorable == r.ThrowIgnorable
}

func (r *dataRunner) Returns() []ep.Type { return []ep.Type{ep.Wildcard} }
func (r *dataRunner) Run(ctx context.Context, inp, out chan ep.Dataset) (err error) {
	if r.ThrowIgnorable {
		err = ep.ErrIgnorable
	} else {
		err = fmt.Errorf("error %s", r.ThrowOnData)
	}
	for data := range inp {
		out <- data
		if r.ThrowOnData == data.At(data.Width() - 1).Strings()[0] {
			return err
		}
	}
	return nil
}

type nodeAddr struct{}

func (*nodeAddr) Equals(other interface{}) bool {
	_, ok := other.(*nodeAddr)
	return ok
}
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

func (*count) Equals(other interface{}) bool {
	_, ok := other.(*count)
	return ok
}
func (*count) Returns() []ep.Type { return []ep.Type{str} }
func (*count) Run(_ context.Context, inp, out chan ep.Dataset) error {
	for data := range inp {
		out <- ep.NewDataset(strs{fmt.Sprintf("%d", data.Len())})
	}
	return nil
}

type upper struct{}

func (*upper) Equals(other interface{}) bool {
	_, ok := other.(*upper)
	return ok
}
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
func (*upper) Scopes() ep.StringsSet {
	return ep.StringsSet{"upper_scope": struct{}{}}
}

type question struct {
	// called flag helps tests ensure runner was/wasn't called
	called bool
}

func (*question) Equals(other interface{}) bool {
	_, ok := other.(*question)
	return ok
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

type addInts struct{}

func (*addInts) Equals(other interface{}) bool {
	_, ok := other.(*addInts)
	return ok
}
func (*addInts) Returns() []ep.Type { return []ep.Type{integer} }
func (*addInts) BatchFunction() ep.BatchFunction {
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

type negateInt struct{}

func (*negateInt) Equals(other interface{}) bool {
	_, ok := other.(*negateInt)
	return ok
}
func (*negateInt) Returns() []ep.Type { return []ep.Type{integer} }
func (*negateInt) BatchFunction() ep.BatchFunction {
	return func(data ep.Dataset) (ep.Dataset, error) {
		d0 := data.At(0).(integers)
		res := make(integers, data.Len())
		for i := range res {
			res[i] = -1 * d0[i]
		}
		return ep.NewDataset(res), nil
	}
}

type mulIntBy2 struct{}

func (*mulIntBy2) Equals(other interface{}) bool {
	_, ok := other.(*mulIntBy2)
	return ok
}
func (*mulIntBy2) Returns() []ep.Type { return []ep.Type{integer} }
func (*mulIntBy2) BatchFunction() ep.BatchFunction {
	return func(data ep.Dataset) (ep.Dataset, error) {
		d0 := data.At(0).(integers)
		res := make(integers, data.Len())
		for i := range res {
			res[i] = d0[i] * 2
		}
		return ep.NewDataset(res), nil
	}
}

type localSort struct {
	SortingCols []ep.SortingCol
}

func (l *localSort) Equals(other interface{}) bool {
	r, ok := other.(*localSort)
	if !ok || len(l.SortingCols) != len(r.SortingCols) {
		return false
	}

	for i, col := range l.SortingCols {
		if !col.Equals(r.SortingCols[i]) {
			return false
		}
	}

	return true
}

func (*localSort) Returns() []ep.Type { return []ep.Type{ep.Wildcard, str} }
func (ls *localSort) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	builder := ep.NewDatasetBuilder()
	hasData := false
	// consume entire local input before sorting all together
	for data := range inp {
		builder.Append(data)
		hasData = true
	}
	var selfData ep.Data
	if hasData {
		selfData = builder.Data()
	}
	size := selfData.Len()
	if size > 0 {
		selfDataset := selfData.(ep.Dataset)
		ep.Sort(selfDataset, ls.SortingCols)

		// once sorted, split to batches again to stream batches to distributedOrder
		i := 0
		for ; i < size/batchSize; i++ {
			out <- selfData.Slice(i*batchSize, (i+1)*batchSize).(ep.Dataset)
		}
		if i*batchSize < size {
			out <- selfData.Slice(i*batchSize, size).(ep.Dataset)
		}
	}
	return nil
}

type runOther struct {
	runner ep.Runner
}

func (r *runOther) Equals(other interface{}) bool {
	o, ok := other.(*runOther)
	return ok && r.runner.Equals(o.runner)
}

func (r *runOther) Returns() []ep.Type { return r.runner.Returns() }
func (r *runOther) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	innerInp := make(chan ep.Dataset)
	innerOut := make(chan ep.Dataset)

	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(innerOut)
		// running without ep.Run so the inp won't be draining
		err = r.runner.Run(ctx, innerInp, innerOut)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range inp {
			innerInp <- data
		}
		close(innerInp)
	}()

	for data := range innerOut {
		out <- data
	}

	wg.Wait()
	return err
}

type runnerWithSize struct {
	ep.Runner
	size int
}

func (r *runnerWithSize) Equals(other interface{}) bool {
	o, ok := other.(*runnerWithSize)
	return ok && r.Runner.Equals(o.Runner) && r.size == o.size
}

func (r *runnerWithSize) Returns() []ep.Type { return []ep.Type{ep.Wildcard} }
func (r *runnerWithSize) ApproxSize() int {
	return r.size
}
