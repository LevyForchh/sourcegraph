package converter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPrune(t *testing.T) {
	gitContentsOracle := map[string][]string{
		"root":     {"sub/", "foo.go", "bar.go"},
		"root/sub": {"sub/baz.go"},
	}

	mockGetChildrenFunc := func(dirnames []string) (map[string][]string, error) {
		out := map[string][]string{}
		for _, dirname := range dirnames {
			out[dirname] = gitContentsOracle[dirname]
		}

		return out, nil
	}

	state := &CorrelationState{
		DocumentData: map[string]DocumentData{
			"d01": {URI: "foo.go"},
			"d02": {URI: "bar.go"},
			"d03": {URI: "sub/baz.go"},
			"d04": {URI: "foo.generated.go"},
			"d05": {URI: "foo.generated.go"},
		},
		DefinitionData: map[string]defaultIDSetMap{
			"x01": {"d01": {}, "d04": {}},
			"x02": {"d02": {}},
		},
		ReferenceData: map[string]defaultIDSetMap{
			"x03": {"d02": {}},
			"x04": {"d02": {}, "d05": {}},
		},
	}

	if err := Prune(mockGetChildrenFunc, "root", state); err != nil {
		t.Fatalf("unexpected error pruning state: %s", err)
	}

	expectedState := &CorrelationState{
		DocumentData: map[string]DocumentData{
			"d01": {URI: "foo.go"},
			"d02": {URI: "bar.go"},
			"d03": {URI: "sub/baz.go"},
		},
		DefinitionData: map[string]defaultIDSetMap{
			"x01": {"d01": {}},
			"x02": {"d02": {}},
		},
		ReferenceData: map[string]defaultIDSetMap{
			"x03": {"d02": {}},
			"x04": {"d02": {}},
		},
	}
	if diff := cmp.Diff(expectedState, state); diff != "" {
		t.Errorf("unexpected state (-want +got):\n%s", diff)
	}
}
