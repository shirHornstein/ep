package ep

var _ = registerGob(dataset{}, &datasetType{})

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

// Width of the dataset (number of columns)
func (set dataset) Width() int {
    return len(set)
}

// Len of the dataset (number of rows). Assumed that all columns are of equal
// length, and thus only checks the first.
func (set dataset) Len() int {
    if set == nil || len(set) == 0 {
        return 0
    }

    return set[0].Len()
}

// At returns the Data at index i
func (set dataset) At(i int) Data {
    return set[i]
}

// Append a data (assumed by interface spec to be a Dataset)
func (set dataset) Append(data Data) Data {
    other := data.(dataset)
    if set == nil {
        return other
    } else if other == nil {
        return set
    }

    if len(set) != len(other) {
        panic("Unable to append mismatching number of columns")
    }

    for i := range set {
        set[i] = set[i].Append(other[i])
    }

    return set
}

// see sort.Interface. Uses the last column.
func (set dataset) Less(i, j int) bool {
    if set == nil || len(set) == 0 {
        return false
    }

    return set[len(set) - 1].Less(i, j)
}

// see sort.Interface
func (set dataset) Swap(i, j int) {
    for _, data := range set {
        data.Swap(i, j)
    }
}

// see Data.Slice. Returns a dataset.
func (set dataset) Slice(start, end int) Data {
    res := make(dataset, len(set))
    for i := range set {
        res[i] = set[i].Slice(start, end)
    }
    return res
}

// see Data.Strings(). Currently not implemented.
func (set dataset) Strings() []string {
    panic("Dataset cannot be cast to strings")
}

// see Data.Data
func (set dataset) Type() Type {
    return &datasetType{}
}

type datasetType struct {}
func (sett *datasetType) Name() string { return "Dataset" }
func (sett *datasetType) Data(n uint) Data { return make(dataset, n) }
