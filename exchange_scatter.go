package ep

import (
	"github.com/satori/go.uuid"
	"io"
)

// Scatter returns an exchange Runner that scatters its input uniformly to
// all other nodes such that the received datasets are dispatched in a round-
// robin to the nodes
func Scatter() Runner {
	uid, _ := uuid.NewV4()
	return &exchange{UID: uid.String(), Type: scatter}
}

func (ex *exchange) encodeScatter(data Dataset) error {
	amountOfPeers := len(ex.encs)
	peersWithLargerBatch := data.Len() % amountOfPeers
	batchSize := data.Len() / amountOfPeers
	start := 0
	var err error

	// send remainder batch size, i.e. send the peersWithLargerBatch (modulo) data size
	for i := 0; i < peersWithLargerBatch; i++ {
		end := start + batchSize + 1
		err = ex.encodeNext(data.Slice(start, end))
		if err != nil {
			return err
		}
		start = end
	}

	// send data batch size
	for i := peersWithLargerBatch; i < amountOfPeers && start < data.Len(); i++ {
		end := start + batchSize
		err = ex.encodeNext(data.Slice(start, end))
		if err != nil {
			return err
		}
		start = end
	}
	return err
}

// encodeNext encodes an object to the next destination connection in a round robin
func (ex *exchange) encodeNext(e interface{}) error {
	if len(ex.encs) == 0 {
		return io.ErrClosedPipe
	}

	req := &req{e}
	ex.encsNext = (ex.encsNext + 1) % len(ex.encs)
	return ex.encs[ex.encsNext].Encode(req)
}
