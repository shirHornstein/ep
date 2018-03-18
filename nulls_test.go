package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"testing"
)

func TestNullsInvariant(t *testing.T) {
	eptest.VerifyDataInterfaceInvariant(t, ep.Null.Data(10))
}
