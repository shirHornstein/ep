package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
)

func ExampleDataset_Duplicate() {
	var dataset1 = ep.NewDataset(strs([]string{"hello", "world"}))
	dataset2 := dataset1.Duplicate(1).(ep.Dataset)
	d2 := dataset2.At(0).(strs)
	d2[0] = "foo"
	d2[1] = "bar"
	fmt.Println(dataset2.At(0)) // clone modified
	fmt.Println(dataset1.At(0)) // original left intact

	// Output:
	// [foo bar]
	// [hello world]
}

func ExampleCut() {
	var d ep.Data = strs([]string{"hello", "world", "foo", "bar"})
	data := ep.Cut(d, 1, 3)
	fmt.Println(data.Strings())

	// Output:
	// [[hello] [world foo] [bar]]
}
