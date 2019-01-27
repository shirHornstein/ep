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
		rowsLeft := b.Size - buffer.Len()

		for data.Len() > b.Size {
			if buffer.Len() == 0 {
				out <- data.Slice(0, b.Size).(Dataset)
				data = data.Slice(b.Size, data.Len()).(Dataset)
			} else {
				delta := data.Slice(0, rowsLeft)
				out <- buffer.Append(delta).(Dataset)

				data = data.Slice(rowsLeft, data.Len()).(Dataset)
				buffer = NewDataset()
				rowsLeft = b.Size
			}
		}

		if data.Len() > rowsLeft {
			delta := data.Slice(0, rowsLeft)
			out <- buffer.Append(delta).(Dataset)

			buffer = data.Slice(rowsLeft, data.Len()).(Dataset)
		} else {
			buffer = buffer.Append(data).(Dataset)
		}
	}
	// leftover buffer can be less than Size
	if buffer.Len() > 0 {
		out <- buffer
	}
	return nil
}
