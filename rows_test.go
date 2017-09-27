package ep

import (
	"context"
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestRows(t *testing.T) {
	data := NewDataset(strs([]string{"hello", "world"}))
	runner := Pipeline(&dataRunner{data}, &upper{})
	rows := Rows(context.Background(), runner).(driver.Rows)
	cols := rows.Columns()
	require.Equal(t, 1, len(cols))
	require.Equal(t, "upper", cols[0])

	dest := make([]driver.Value, 1)
	res := []driver.Value{}
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		res = append(res, dest[0])
	}

	require.Equal(t, 2, len(res))
	require.Equal(t, "HELLO", res[0])
	require.Equal(t, "WORLD", res[1])
}
