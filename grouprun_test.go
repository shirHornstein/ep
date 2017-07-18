package ep

import (
	"fmt"
	"sort"
	"context"
)

type Count struct {}
func (*Count) Returns() []Type { return []Type{Str} }
func (*Count) Run(_ context.Context, inp, out chan Dataset) error {
	c := 0
	for data := range inp {
		c += data.Len()
	}

	out <- NewDataset(Strs{fmt.Sprintf("%d", c)})
	return nil
}

func ExampleGroupRun() {
	runner := GroupRun(&Count{})
	data := NewDataset(Strs([]string{"hello", "world", "hello"}))
	data, err := testRun(runner, data)
	sort.Sort(data) // order is not guaranteed
	fmt.Println(data, err) // Output: [[1 2]] <nil>
}
