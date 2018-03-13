package ep

import (
	"context"
	"fmt"
	"strings"
)

type upper struct{}

func (*upper) Returns() []Type { return []Type{SetAlias(str, "upper")} }
func (*upper) Run(_ context.Context, inp, out chan Dataset) error {
	for data := range inp {
		if data.At(0).Type() == Null {
			out <- data
			continue
		}

		res := make(strs, data.Len())
		for i, v := range data.At(0).(strs) {
			res[i] = strings.ToUpper(v)
		}
		out <- NewDataset(res)
	}
	return nil
}

type question struct {
	// called flag helps tests ensure runner was/wasn't called
	called bool
}

func (*question) Returns() []Type { return []Type{SetAlias(str, "question")} }
func (q *question) Run(_ context.Context, inp, out chan Dataset) error {
	q.called = true
	for data := range inp {
		if data.At(0).Type() == Null {
			out <- data
			continue
		}

		res := make(strs, data.Len())
		for i, v := range data.At(0).(strs) {
			res[i] = "is " + v + "?"
		}
		out <- NewDataset(res)
	}
	return nil
}

func ExampleRunner() {
	upper := &upper{}
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(upper, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[HELLO WORLD]] <nil>
}

func ExampleRunnerFilterable_Filter() {
	runner := Project(&question{}, &upper{}, &question{}).(RunnerFilterable)
	err := runner.Filter([]bool{false, true, false})
	fmt.Println(err)

	data := NewDataset(strs([]string{"hello", "world"}))
	data, err = TestRunner(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// <nil>
	// [[] [HELLO WORLD] []] <nil>
}
