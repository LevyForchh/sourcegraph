package existence

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseDirectoryChildrenRoot(t *testing.T) {
	dirnames := []string{""}
	paths := []string{
		".github",
		".gitignore",
		"LICENSE",
		"README.md",
		"cmd",
		"go.mod",
		"go.sum",
		"internal",
		"protocol",
	}

	expected := map[string][]string{
		"": paths,
	}

	if diff := cmp.Diff(expected, parseDirectoryChildren(dirnames, paths)); diff != "" {
		t.Errorf("unexpected directory children result (-want +got):\n%s", diff)
	}
}

func TestParseDirectoryChildrenNonRoot(t *testing.T) {
	dirnames := []string{"cmd/", "protocol/"}
	paths := []string{
		"cmd/lsif-go",
		"protocol/protocol.go",
		"protocol/writer.go",
	}

	expected := map[string][]string{
		"cmd/":      []string{"cmd/lsif-go"},
		"protocol/": []string{"protocol/protocol.go", "protocol/writer.go"},
	}

	if diff := cmp.Diff(expected, parseDirectoryChildren(dirnames, paths)); diff != "" {
		t.Errorf("unexpected directory children result (-want +got):\n%s", diff)
	}
}

func TestCleanDirectoriesForLsTree(t *testing.T) {
	args := []string{"", "foo", "bar/", "baz"}
	actual := cleanDirectoriesForLsTree(args)
	expected := []string{".", "foo/", "bar/", "baz/"}

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("unexpected ls-tree args (-want +got):\n%s", diff)
	}
}
