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

func (b *batch) Returns() []Type { return []Type{Wildcard} }
func (b *batch) Run(ctx context.Context, inp, out chan Dataset) error {
	buffer := NewDataset()

	for data := range inp {
		var sliceStart, sliceEnd int
		dataLen := data.Len()
		for dataLen > sliceEnd {
			bufferLen := buffer.Len()
			rowsLeft := b.Size - bufferLen
			sliceEnd = sliceStart + rowsLeft
			if sliceEnd > dataLen {
				sliceEnd = dataLen
			}
			delta := data.Slice(sliceStart, sliceEnd).(Dataset)

			if bufferLen == 0 && delta.Len() == b.Size {
				out <- delta
			} else {
				buffer = buffer.Append(delta).(Dataset)
				if buffer.Len() == b.Size {
					out <- buffer
					buffer = NewDataset()
				}
			}
			sliceStart = sliceEnd
		}
	}
	// leftover buffer can be less than Size
	if buffer.Len() > 0 {
		out <- buffer
	}
	return nil
}
