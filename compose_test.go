package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompose(t *testing.T) {
	c1 := integers{1, 2, 3, 4, 5}
	c2 := integers{10, 20, 30, 40, 50}
	input := ep.NewDataset(c1, c2)
	expectedScopes := ep.StringsSet{
		"a": {},
	}

	t.Run("single Composable", func(t *testing.T) {
		runner := ep.Compose(
			[]ep.Type{integer},
			ep.StringsSet{"a": struct{}{}},
			&addInts{},
		)
		expected := []string{"11", "22", "33", "44", "55"}

		res, err := eptest.Run(runner, input)
		require.NoError(t, err)
		require.Equal(t, 1, res.Width())
		require.Equal(t, input.Len(), res.Len())
		require.Equal(t, expected, res.At(0).Strings())
		require.Equal(t, expectedScopes, runner.(ep.ScopesRunner).Scopes())
	})

	t.Run("multiple Composables", func(t *testing.T) {
		runner := ep.Compose(
			[]ep.Type{integer},
			ep.StringsSet{"b": struct{}{}},
			&addInts{}, &negateInt{},
		)
		expected := []string{"-11", "-22", "-33", "-44", "-55"}

		res, err := eptest.Run(runner, input)
		require.NoError(t, err)
		require.Equal(t, 1, res.Width())
		require.Equal(t, input.Len(), res.Len())
		require.Equal(t, expected, res.At(0).Strings())
		require.NotEqual(t, expectedScopes, runner.(ep.ScopesRunner).Scopes())
	})
}

func TestComposeProject(t *testing.T) {
	col := integers{1, 2, 3, 4, 5}
	input := ep.NewDataset(col)

	proj1 := &negateInt{}
	proj2 := ep.ComposeProject(&mulIntBy2{}, &negateInt{})
	proj3 := &mulIntBy2{}

	project := ep.ComposeProject(proj1, proj2, proj3)
	batchFunction := project.BatchFunction()

	expected1 := []string{"-1", "-2", "-3", "-4", "-5"}
	expected2 := []string{"2", "4", "6", "8", "10"}
	expected3 := []string{"-1", "-2", "-3", "-4", "-5"}
	expected4 := []string{"2", "4", "6", "8", "10"}

	res, err := batchFunction(input)
	require.NoError(t, err)
	require.Equal(t, 4, res.Width())
	require.Equal(t, input.Len(), res.Len())

	require.Equal(t, expected1, res.At(0).Strings())
	require.Equal(t, expected2, res.At(1).Strings())
	require.Equal(t, expected3, res.At(2).Strings())
	require.Equal(t, expected4, res.At(3).Strings())
}

func TestComposeProject_creation(t *testing.T) {
	t.Run("no composable", func(t *testing.T) {
		require.Panics(t, func() { ep.ComposeProject() })
	})

	t.Run("single composable without projecting", func(t *testing.T) {
		cmp := &mulIntBy2{}
		project := ep.ComposeProject(cmp)
		require.IsType(t, cmp, project)
	})
}
