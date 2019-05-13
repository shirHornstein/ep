package ep

import (
	"context"
)

var _ = registerGob(&batch{})

// Batch makes sure that its input continues the flow in batches of equal size,
// except for the last one, which might be shorter. Input data is not modified.
// Data flow may be blocked until there is enough input to produce a batch of a
// requested size. If the input is too large, it is broken into multiple
// batches.
func Batch(size int) Runner {
	return &batch{size}
}

type batch struct {
	Size int
}

func (b *batch) Equals(other interface{}) bool {
	_, ok := other.(*batch)
	return ok
}

func (b *batch) Returns() []Type { return []Type{Wildcard} }
func (b *batch) Run(ctx context.Context, inp, out chan Dataset) error {
	buffer := NewDatasetBuilder()
	var bufferLen int

	for data := range inp {
		var sliceStart, sliceEnd int
		dataLen := data.Len()
		for dataLen > sliceEnd {
			rowsLeft := b.Size - bufferLen
			sliceEnd = sliceStart + rowsLeft
			if sliceEnd > dataLen {
				sliceEnd = dataLen
			}
			delta := data
			if dataLen > rowsLeft || sliceStart > 0 {
				delta = data.Slice(sliceStart, sliceEnd).(Dataset)
			}

			deltaLen := delta.Len()
			if deltaLen == b.Size {
				out <- delta
			} else {
				buffer.Append(delta)
				bufferLen += deltaLen
				if bufferLen == b.Size {
					out <- buffer.Data().(Dataset)
					buffer = NewDatasetBuilder()
					bufferLen = 0
				}
			}
			sliceStart = sliceEnd
		}
	}
	// leftover buffer can be less than Size
	if bufferLen > 0 {
		out <- buffer.Data().(Dataset)
	}
	return nil
}
