package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
)

func ExampleMapInpToOut() {
	// breakChars is a runner that breaks the input strings into characters (one
	// character per row).
	r := ep.MapInpToOut(&breakChars{})
	data := ep.NewDataset(strs([]string{"XY", "ABC"}))
	data, err := eptest.Run(r, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[XY XY ABC ABC ABC] [X Y A B C]] <nil>
}
