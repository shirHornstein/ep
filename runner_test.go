package ep

import (
	"context"
	"fmt"
	"strings"
)

type upper struct{}

func (*upper) Returns() []Type {
	return []Type{Modify(str, "As", "upper")}
}
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

type question struct{}

func (*question) Returns() []Type {
	return []Type{Modify(str, "As", "question")}
}
func (*question) Run(_ context.Context, inp, out chan Dataset) error {
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
	fmt.Println(data, err) // [[HELLO WORLD]] <nil>

	// Output: [[HELLO WORLD]] <nil>
}
