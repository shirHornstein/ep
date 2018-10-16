package ep

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProject_creationSingleRunner(t *testing.T) {
	runner := Project(Gather())
	_, isProject := runner.(project)
	require.False(t, isProject)

	passThrough := Project(PassThrough())
	_, isProject = passThrough.(project)
	require.False(t, isProject)

	projectWithPipeline := Project(Pipeline(Project(Scatter(), Scatter()), Gather()))
	_, isProject = projectWithPipeline.(project)
	require.False(t, isProject)
}

func TestProject_creationFlat(t *testing.T) {
	runner := Project(
		Project(Gather(), Scatter()),
		Project(Scatter(), Project(Scatter(), Scatter()), PassThrough()),
		PassThrough(),
	)

	p, isProject := runner.(project)
	require.True(t, isProject)
	require.Equal(t, 7, len(p))

	nestedProject := Project(Project(Scatter(), Scatter()))
	p, isProject = nestedProject.(project)
	require.True(t, isProject)
	require.Equal(t, 2, len(p))
	_, isProject = p[0].(project)
	require.False(t, isProject)
}

func TestProject_creationUnFlatPipeline(t *testing.T) {
	runner := Pipeline(Gather(), Project(Scatter(), Scatter()), Project(Scatter(), Scatter()))
	runner = Project(runner, Gather())

	p, isProject := runner.(project)
	require.True(t, isProject)
	require.Equal(t, 2, len(p))
}
