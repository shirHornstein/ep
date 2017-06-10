package ep

import (
    "sort"
)

// Data is an abstract interface representing a set of typed values. Implement
// it for each type of data that you need to support
type Data interface {
    sort.Interface // data is sortable

    // Type returns the data type of the contained values
    Type() Type

    // Slice returns a new data object containing only the values from the start
    // to end indices
    Slice(start, end int) Data

    // Append another data object to this one. It can be assumed that the type
    // of the input data is similar to the current one, otherwise it's safe to
    // panic
    Append(Data) Data

    // ToStrings returns the string representation of all of the Data values
    ToStrings() []string

    // Clone returns a new Data object containing the same values as the
    // current one
    Clone() Data
}
