package ep_test

import (
	"context"
	"errors"
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestRun_drainInput(t *testing.T) {
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	var err error

	// errRunner reads only one batch from its input
	r := &errRunner{errors.New("err"), "err"}

	go ep.Run(context.Background(), r, inp, out, nil, &err)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	close(inp)

	for range out {
	}

	require.Error(t, err)
	require.Equal(t, "err", err.Error())
}

func TestRun_closeOut(t *testing.T) {
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	var err error

	var wg sync.WaitGroup
	r := ep.PassThrough()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ep.Run(context.Background(), r, inp, out, nil, &err)
	}()
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	close(inp)

	result := <-out

	wg.Wait()

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Panics(t, func() { close(out) }, "expected ep.Run to close out channel")
}

func TestRun_callCancel(t *testing.T) {
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	var err error
	canceledCalled := false
	cancel := func() {
		canceledCalled = true
	}

	r := &errRunner{errors.New("err"), "err"}

	go ep.Run(context.Background(), r, inp, out, cancel, &err)
	inp <- ep.NewDatasetTypes([]ep.Type{str}, 10)
	close(inp)

	for range out {
	}

	require.Error(t, err)
	require.Equal(t, "err", err.Error())
	require.True(t, canceledCalled)
}
