package ep

// import (
//     "testing"
// )

func ExampleData() {
    type StringsData []string
    func (d StringsData) Len() { return len(d) }
    func (d StringsData) Less(i, j int) { return d[i] < d[j] }
    func (d StringsData) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
    func (d StringsData) Slice(s, e int) { return d[s:e] }
    func (d StringsData) Append(other Data) { return append(d, other.(StringsData)...) }
    func (d StringsData) Strings() []string { return d }
}
