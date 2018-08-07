package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
)

func ExampleCut() {
	var d ep.Data = strs([]string{"hello", "world", "foo", "bar"})
	datasets := ep.Cut(d, 1, 3)
	for _, data := range datasets {
		fmt.Println(data.Strings())
	}

	// Output:
	// [hello]
	// [world foo]
	// [bar]
}
