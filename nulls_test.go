package ep

import (
	"testing"
)

func TestNullsInvariant(t *testing.T) {
	VerifyDataInvariant(t, Null.Data(10))
}
