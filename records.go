package ep

// Record data type, implements Type
// Records represents a single/partial row by untyped array of fields
var Record = &recordType{}
var _ = Types.Register("record", Record)

const emptyRecordStr = "()"

type recordType struct{}

func (t *recordType) String() string     { return t.Name() }
func (*recordType) Name() string         { return "record" }
func (*recordType) Data(n int) Data      { return &Records{dataset{}} }
func (*recordType) DataEmpty(n int) Data { return &Records{dataset{}} }

// Records implements Dataset to store a list of record values,
// each record composed of typed items
type Records struct {
	D dataset // must be public for being gob-able
}

// NewRecords creates a new Records object that holds record values
func NewRecords(data ...Data) *Records {
	return &Records{dataset(data)}
}

// Width implements Dataset
func (vs *Records) Width() int {
	return vs.D.Width()
}

// At implements Dataset
func (vs *Records) At(i int) Data {
	return vs.D.At(i)
}

// Expand implements Dataset
func (vs *Records) Expand(other Dataset) (Dataset, error) {
	panic("runtime error: records is not expandable")
}

// Split implements Dataset
func (vs *Records) Split(secondWidth int) (Dataset, Dataset) {
	panic("runtime error: records is not splitable")
}

// Type implements Data
func (*Records) Type() Type { return Record }

// Len implements sort.Interface and Data
func (vs *Records) Len() int {
	return vs.D.Len()
}

// Less implements sort.Interface and Data
func (vs *Records) Less(i, j int) bool {
	return vs.D.Less(i, j)
}

// Swap implements sort.Interface and Data
func (vs *Records) Swap(i, j int) {
	vs.D.Swap(i, j)
}

// LessOther implements Data
// By default sorts by first column ascending
func (vs *Records) LessOther(thisRow int, other Data, otherRow int) bool {
	data := other.(*Records)
	return vs.At(0).LessOther(thisRow, data.At(0), otherRow)
}

// Slice implements Data
func (vs *Records) Slice(start, end int) Data {
	return &Records{vs.D.Slice(start, end).(dataset)}
}

// Append implements Data
func (vs *Records) Append(other Data) Data {
	d := other.(*Records)
	if vs.Width() != d.D.Width() {
		panic("Unable to append records with different width")
	}
	res := make(dataset, vs.Width())
	for i, col := range vs.D {
		res[i] = col.Append(d.D.At(i))
	}
	return &Records{res}
}

// Duplicate implements Data
func (vs *Records) Duplicate(t int) Data {
	return &Records{vs.D.Duplicate(t).(dataset)}
}

// IsNull implements Data
func (vs *Records) IsNull(index int) bool {
	// record considered to be null iff it contains only nulls
	for _, d := range vs.D {
		if !d.IsNull(index) {
			return false
		}
	}
	return true
}

// MarkNull implements Data
func (vs *Records) MarkNull(index int) {
	for _, d := range vs.D {
		d.MarkNull(index)
	}
}

// Nulls implements Data
func (vs *Records) Nulls() []bool {
	res := make([]bool, vs.Len())
	for i := range res {
		res[i] = vs.IsNull(i)
	}
	return res
}

// Equal implements Data
func (vs *Records) Equal(other Data) bool {
	data, ok := other.(*Records)
	if !ok {
		return false
	}

	for i, d := range vs.D {
		if !d.Equal(data.At(i)) {
			return false
		}
	}
	return true
}

// Copy implements Data
func (vs *Records) Copy(from Data, fromRow, toRow int) {
	fromRecords := from.(*Records)
	vs.D.Copy(fromRecords.D, fromRow, toRow)
}

// Strings implements Data
func (vs *Records) Strings() []string {
	res := make([]string, vs.Len())
	if vs.Width() == 0 {
		for i := range res {
			res[i] = emptyRecordStr
		}
		return res
	}

	for _, col := range vs.D {
		strs := col.Strings()
		for i, s := range strs {
			res[i] += s + ","
		}
	}
	for i, s := range res {
		res[i] = "(" + s[:len(s)-1] + ")"
	}
	return res
}
