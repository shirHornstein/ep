package ep

import (
	"context"
	"fmt"
	"sort"
)

type count struct{}

func (*count) Returns() []Type { return []Type{str} }
func (*count) Run(_ context.Context, inp, out chan Dataset) error {
	c := 0
	for data := range inp {
		c += data.Len()
	}

	out <- NewDataset(strs{fmt.Sprintf("%d", c)})
	return nil
}

func ExampleGroupRun() {
	runner := GroupRun(&count{})
	data := NewDataset(strs([]string{"hello", "world", "hello"}))
	data, err := TestRunner(runner, data)
	sort.Sort(data)        // order is not guaranteed
	fmt.Println(data, err) // Output: [[1 2]] <nil>
}
