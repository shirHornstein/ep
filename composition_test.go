package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestComposition(t *testing.T) {
	c1 := integers{1, 2, 3, 4, 5}
	c2 := integers{10, 20, 30, 40, 50}
	input := ep.NewDataset(c1, c2)

	t.Run("OnePiece", func(t *testing.T) {
		runner := ep.Compose(
			[]ep.Type{integer},
			&addInts{},
		)
		expected := []string{"11", "22", "33", "44", "55"}

		res, err := eptest.Run(runner, input)
		require.NoError(t, err)
		require.Equal(t, 1, res.Width())
		require.Equal(t, input.Len(), res.Len())
		require.Equal(t, expected, res.At(0).Strings())
	})

	t.Run("TwoPieces", func(t *testing.T) {
		runner := ep.Compose(
			[]ep.Type{integer},
			&addInts{}, &negateInt{},
		)
		expected := []string{"-11", "-22", "-33", "-44", "-55"}

		res, err := eptest.Run(runner, input)
		require.NoError(t, err)
		require.Equal(t, 1, res.Width())
		require.Equal(t, input.Len(), res.Len())
		require.Equal(t, expected, res.At(0).Strings())
	})
}

func TestComposeProject(t *testing.T) {
	col := integers{1, 2, 3, 4, 5}
	input := ep.NewDataset(col)

	comp1 := ep.Compose(
		[]ep.Type{integer},
		&negateInt{}, &mulIntBy2{},
	).(ep.Composable)
	comp2 := ep.Compose(
		[]ep.Type{integer},
		&mulIntBy2{}, &mulIntBy2{}, &negateInt{},
	).(ep.Composable)
	project := ep.ComposeProject(comp1, comp2)

	composition := ep.Compose(
		[]ep.Type{integer, integer},
		project, &addInts{},
	)
	expected := []string{"-6", "-12", "-18", "-24", "-30"}

	res, err := eptest.Run(composition, input)
	require.NoError(t, err)
	require.Equal(t, 1, res.Width())
	require.Equal(t, input.Len(), res.Len())
	require.Equal(t, expected, res.At(0).Strings())
}
