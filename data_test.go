package ep

// import (
//     "testing"
// )

type StringsData []string
func (d StringsData) Len() int {
    return len(d)
}

func (d StringsData) Less(i, j int) bool { return d[i] < d[j] }
func (d StringsData) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d StringsData) Slice(s, e int) Data { return d[s:e] }
func (d StringsData) Append(other Data) Data { return append(d, other.(StringsData)...) }
func (d StringsData) Strings() []string { return d }

func ExampleData() {

}
