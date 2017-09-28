package ep

import "testing"

func TestDataInterface(t *testing.T) {
	dataset := NewDataset(Null.Data(10), str.Data(10))
	VerifyDataInvariant(t, dataset)
}
