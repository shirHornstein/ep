package ep

var _ = registerGob(dataset{})

// Dataset is a composite Data interface, containing several internal Data
// objects. It's a Data in itself, but allows traversing and manipulating the
// contained Data intstances
type Dataset interface {
    Data

    // Width returns the number of Data instances (columns) in the set
    Width() int

    // At returns the Data instance at index i
    At(i int) Data
}

type dataset []Data

// NewDataset creates a new Data object that's a horizontal composition of the
// provided Data objects
func NewDataset(data ...Data) Dataset {
    return dataset(data)
}

func (set dataset) Width() int {
    return len(set)
}

func (set dataset) At(i int) Data {
    return set[i]
}
