package ep

// Dataset is a composite Data interface, containing several internal Data
// objects. It's a Data in itself, but allows traversing and manipulating the
// contained Data intstances
type Dataset interface {
    Data

    // Width returns
    Width() int
    At(int) Data
}

type dataset []Data

// NewDataset creates a new Data object that's a horizontal composition of the
// provided Data objects
func NewDataset(data ...Data) Data {
    return dataset(data)
}

func (set dataset) Width() int {
    return len(set)
}

func (set dataset) At(i int) Data {
    return set[i]
}
