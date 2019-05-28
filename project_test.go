package ep_test

import (
	"context"
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func ExampleProject() {
	runner := ep.Project(&upper{}, &question{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [(HELLO,is hello?) (WORLD,is world?)] <nil>
}

func ExampleProject_reversed() {
	runner := ep.Project(&question{}, &upper{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [(is hello?,HELLO) (is world?,WORLD)] <nil>
}

func TestProject_noErrorWithDummies(t *testing.T) {
	runner := ep.Project(&upper{}, &upper{}, &upper{}).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true})

	data1 := ep.NewDataset(strs([]string{"hello", "world"}))
	data2 := ep.NewDataset(strs([]string{"Herzl", "theodor"}))
	data, err := eptest.Run(runner, data1, data2)
	require.NoError(t, err)
	require.Equal(t, 3, data.Width())
	require.Equal(t, 4, data.Len())
}

func TestProject_errorInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinity := &waitForCancel{}
	runner := ep.Project(eptest.NewErrRunner(err), infinity)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinity.IsRunning(), "Infinity go-routine leak")
}

func TestProject_errorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	runner := ep.Project(infinityRunner1, eptest.NewErrRunner(err), infinityRunner2)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
}

func TestProject_errorInThirdRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	runner := ep.Project(infinityRunner1, infinityRunner2, eptest.NewErrRunner(err))
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
}

func TestProject_errorInPipeline(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	infinityRunner3 := &waitForCancel{}
	runner := ep.Project(
		ep.Pipeline(infinityRunner1, infinityRunner2),
		ep.Pipeline(infinityRunner3, eptest.NewErrRunner(err)),
	)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.False(t, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

func TestProject_nested_errorInFirstRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	infinityRunner3 := &waitForCancel{}
	runner := ep.Project(
		ep.Project(infinityRunner3, eptest.NewErrRunner(err)),
		ep.Project(infinityRunner1, infinityRunner2),
	)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.False(t, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

func TestProject_nested_errorInSecondRunner(t *testing.T) {
	err := fmt.Errorf("something bad happened")
	infinityRunner1 := &waitForCancel{}
	infinityRunner2 := &waitForCancel{}
	infinityRunner3 := &waitForCancel{}
	runner := ep.Project(
		ep.Project(infinityRunner1, infinityRunner2),
		ep.Project(infinityRunner3, eptest.NewErrRunner(err)),
	)
	data := ep.NewDataset(str.Data(1))
	_, resErr := eptest.Run(runner, data)

	require.Error(t, resErr)
	require.Equal(t, err.Error(), resErr.Error())
	require.False(t, infinityRunner1.IsRunning(), "Infinity 1 go-routine leak")
	require.False(t, infinityRunner2.IsRunning(), "Infinity 2 go-routine leak")
	require.False(t, infinityRunner3.IsRunning(), "Infinity 3 go-routine leak")
}

// projected runners should return the same number of rows
func TestProject_errorMismatchRows(t *testing.T) {
	runner := ep.Project(&upper{}, &count{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	_, err := eptest.Run(runner, data)
	require.Error(t, err)
	require.Equal(t, "mismatched number of rows", err.Error())
}

func TestProject_errorMismatchRowsWithDummies(t *testing.T) {
	runner := ep.Project(&upper{}, &upper{}, &upper{}, &count{}).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true, true})

	data1 := ep.NewDataset(strs([]string{"hello", "world"}))
	data2 := ep.NewDataset(strs([]string{"Herzl"}))
	_, err := eptest.Run(runner, data1, data2)
	require.Error(t, err)
	require.Equal(t, "mismatched number of rows", err.Error())
}

func TestProject_Filter(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := ep.Project(q1, &upper{}, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, true, true})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.True(t, q2.called)
	require.Equal(t, "[(HELLO,is hello?) (WORLD,is world?)]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_all(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	runner := ep.Project(q1, &upper{}, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_allWithNested(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	internalProject := ep.Project(q1, &upper{}).(ep.FilterRunner)
	runner := ep.Project(internalProject, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, -1, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.Equal(t, "[]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalPartial(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	q3 := &question{}
	q4 := &question{}
	internalProject := ep.Project(q2, &upper{}, q3).(ep.FilterRunner)
	runner := ep.Project(q1, internalProject, q4).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true, true, false})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 5, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.False(t, q2.called)
	require.True(t, q3.called)
	require.False(t, q4.called)
	require.Equal(t, "[(HELLO,is hello?) (WORLD,is world?)]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_Filter_nestedWithInternalAll(t *testing.T) {
	q1 := &question{}
	q2 := &question{}
	internalProject := ep.Project(q1, &upper{}).(ep.FilterRunner)
	runner := ep.Project(internalProject, q2).(ep.FilterRunner)
	runner.Filter([]bool{false, false, true})
	data := ep.NewDataset(strs([]string{"hello", "world"}))

	data, err := eptest.Run(runner, data)
	require.NoError(t, err)

	require.Equal(t, 3, data.Width())
	require.Equal(t, 2, data.Len())
	require.False(t, q1.called)
	require.True(t, q2.called)
	require.Equal(t, "[(is hello?) (is world?)]", fmt.Sprintf("%+v", data.Strings()))
}

func TestProject_ApproxSize(t *testing.T) {
	t.Run("none is ApproxSizer", func(t *testing.T) {
		r1 := ep.PassThrough()
		r2 := &question{}

		p := ep.Project(r1, r2)
		sizer, ok := p.(ep.ApproxSizer)
		require.True(t, ok)
		require.Equal(t, ep.UnknownSize, sizer.ApproxSize())
	})

	t.Run("part is ApproxSizer", func(t *testing.T) {
		r1 := ep.PassThrough()
		r2 := &runnerWithSize{size: 42}

		p := ep.Project(r1, r2)
		sizer, ok := p.(ep.ApproxSizer)
		require.True(t, ok)
		require.Equal(t, ep.UnknownSize, sizer.ApproxSize())
	})

	t.Run("all are ApproxSizer", func(t *testing.T) {
		r1 := &runnerWithSize{size: 42}
		r2 := &runnerWithSize{size: 11}

		p := ep.Project(r1, r2)
		sizer, ok := p.(ep.ApproxSizer)
		require.True(t, ok)
		require.Equal(t, 42+11, sizer.ApproxSize())
	})
}

func TestProject_drainOriginInput(t *testing.T) {
	project := ep.Project(ep.PassThrough(), ep.PassThrough())

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	runner := &runOther{project}

	data := ep.NewDataset(strs{"data"})
	inp := make(chan ep.Dataset)
	out := make(chan ep.Dataset)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		ep.Run(ctx, runner, inp, out, cancel, &err)
		require.NoError(t, err)
	}()

	inp <- data
	inp <- data

	cancel()

	go func() {
		for range out {
		}
	}()

	inp <- data
	inp <- data
	inp <- data
	inp <- data
	inp <- data

	close(inp)

	wg.Wait()
}

func BenchmarkProjectImpl(b *testing.B) {
	b.Run("2runners", func(b *testing.B) {
		runner := ep.Project(&upper{}, &question{})
		data := ep.NewDataset(strs([]string{"sUndAy", "mOnDay", "TueSdAY", "wednESday", "tHuRSday", "FRIday"}))
		batches := make([]ep.Dataset, 1000)
		for i := 0; i < 1000; i++ {
			batches[i] = data
		}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			data, err := eptest.Run(runner, batches...)
			require.NoError(b, err)
			require.NotNil(b, data)
		}
	})
	b.Run("5runners", func(b *testing.B) {
		runner := ep.Project(&question{}, &upper{}, &question{}, &upper{}, &question{})
		data := ep.NewDataset(strs([]string{"sUndAy", "mOnDay", "TueSdAY", "wednESday", "tHuRSday", "FRIday"}))
		batches := make([]ep.Dataset, 1000)
		for i := 0; i < 1000; i++ {
			batches[i] = data
		}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			data, err := eptest.Run(runner, batches...)
			require.NoError(b, err)
			require.NotNil(b, data)
		}
	})
	b.Run("100runners", func(b *testing.B) {
		runners := make([]ep.Runner, 100)
		for i := 0; i < 100; i += 5 {
			runners[i] = &question{}
			runners[i+1] = &upper{}
			runners[i+2] = &question{}
			runners[i+3] = &upper{}
			runners[i+4] = &question{}
		}
		runner := ep.Project(runners...)
		data := ep.NewDataset(strs([]string{"sUndAy", "mOnDay", "TueSdAY", "wednESday", "tHuRSday", "FRIday"}))
		batches := make([]ep.Dataset, 1000)
		for i := 0; i < 1000; i++ {
			batches[i] = data
		}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			data, err := eptest.Run(runner, batches...)
			require.NoError(b, err)
			require.NotNil(b, data)
		}
	})
}
