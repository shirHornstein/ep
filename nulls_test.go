package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"testing"
)

func TestNullsInvariant(t *testing.T) {
	eptest.VerifyDataInvariant(t, ep.Null.Data(10))
}
