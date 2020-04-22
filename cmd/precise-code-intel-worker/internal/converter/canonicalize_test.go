package converter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCanonicalizeDocuments(t *testing.T) {
	state := newCorrelationState("")
	// TODO

	expectedState := newCorrelationState("")
	// TODO

	canonicalizeDocuments(state)

	if diff := cmp.Diff(expectedState, state); diff != "" {
		t.Errorf("unexpected state (-want +got):\n%s", diff)
	}
}

func TestCanonicalizeReferenceResults(t *testing.T) {
	// TODO
}

func TestCanonicalizeResultSets(t *testing.T) {
	// TODO
}

func TestCanonicalizeRanges(t *testing.T) {
	// TODO
}
