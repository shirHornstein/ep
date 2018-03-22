package ep

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
)

// Rows creates a new Runner that also implements driver.Rows, useful for cases
// when we need to execute the Runner in a database library. Calling .Next() for
// the first time will Run the underlying Runner with the provided context. This
// runner cannot be distributed and thus should only be used at the top-level
// client-facing code. It's safe to cast the returned Runner to driver.Rows.
func Rows(ctx context.Context, r Runner) Runner {
	return &rows{Runner: r, Ctx: ctx}
}

// rows is a runner that also implements driver.Rows
type rows struct {
	Runner
	Ctx        context.Context
	CancelFunc context.CancelFunc
	Out        chan Dataset
	Buff       [][]string // buffer of received rows waiting for Next()
	Err        error
}

func (r *rows) Run(ctx context.Context, inp, out chan Dataset) error {
	if r.Out != nil {
		if r.Out != out {
			panic("rows already have out")
		}
	} else {
		r.Out = out // save it for Next()
	}
	r.Ctx, r.CancelFunc = context.WithCancel(ctx) // for Close()
	return r.Runner.Run(r.Ctx, inp, out)
}

// see driver.Rows
func (r *rows) Columns() []string {
	cols := []string{}
	for _, t := range r.Returns() {
		alias := GetAlias(t)
		if alias == "" {
			alias = UnnamedColumn
		}
		cols = append(cols, alias)
	}
	return cols
}

// see driver.Rows. Blocks until the runner has indeed finished
func (r *rows) Close() error {
	if r.CancelFunc == nil {
		return nil // it never started.
	}

	r.CancelFunc()

	// wait for it to end.
	return nil // return the error.
}

// see driver.Rows
func (r *rows) Next(dest []driver.Value) error {
	// Not running? start it.
	if r.Out == nil {
		r.Out = make(chan Dataset)

		// kick it off with one empty input
		inp := make(chan Dataset)
		close(inp)
		go func() {
			defer close(r.Out)
			r.Err = r.Run(r.Ctx, inp, r.Out)
		}()
	}

	// if we still have buffered rows, emit the first one and remove it
	if len(r.Buff) > 0 {
		row := r.Buff[0]
		for i := range dest {
			dest[i] = row[i]
		}

		r.Buff = r.Buff[1:]
		return nil
	}

	// read the next batch of rows
	data, ok := <-r.Out
	if !ok && r.Err != nil {
		// This is intentionally not at the beginning of this function, in order
		// to emit all of the buffered rows that arrived before the error
		return r.Err
	} else if !ok {
		return io.EOF
	}

	// build the batch of data
	columnar := [][]string{}
	for i := 0; i < data.Width(); i++ {
		columnar = append(columnar, data.At(i).Strings())
	}

	// transpose the columnar strings to rows of strings for the buffer.
	rows := make([][]string, data.Len())
	for i := range rows {
		row := make([]string, data.Width())
		for j := range row {
			row[j] = columnar[j][i]
		}

		rows[i] = row
	}

	r.Buff = rows

	// now that we have the new buffer, try again.
	return r.Next(dest)
}

// see driver.ColumnTypeDatabaseTypeName(index int)
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	types := r.Returns()
	return strings.ToUpper(types[index].Name())
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}
