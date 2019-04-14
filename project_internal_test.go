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

func TestProject_creationPreservePipeline(t *testing.T) {
	runner := Pipeline(Gather(), Project(Scatter(), Scatter()), Project(Scatter(), Scatter()))
	runner = Project(runner, Gather())

	p, isProject := runner.(project)
	require.True(t, isProject)
	require.Equal(t, 2, len(p))
}

func TestProject_creationFlatComposable(t *testing.T) {
	runner := Project(
		Project(&dummyRunner{}, &dummyRunner{}),
		Project(PassThrough(), Pipeline(&dummyRunner{}, &dummyRunner{})),
		PassThrough(),
	)

	p, isCmpProject := isComposeProject(runner)
	require.True(t, isCmpProject)
	require.Equal(t, 5, len(p))

	_, isProject := isComposeProject(p[0])
	require.False(t, isProject)
}
