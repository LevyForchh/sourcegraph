package existence

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGetDirectoryContentsNoRoot(t *testing.T) {
	gitContentsOracle := map[string][]string{
		"":           {"web/"},
		"web":        {"web/core/", "web/shared/"},
		"web/core":   {"web/core/foo.ts", "web/core/bar.ts", "web/core/baz.ts"},
		"web/shared": {"web/shared/bonk.ts", "web/shared/quux.ts"},
	}

	var requests [][]string
	mockGetChildrenFunc := func(dirnames []string) (map[string][]string, error) {
		out := map[string][]string{}
		for _, dirname := range dirnames {
			out[dirname] = gitContentsOracle[dirname]
		}

		requests = append(requests, dirnames)
		return out, nil
	}

	paths := []string{
		"web/core/foo.ts",
		"web/core/bar.ts",
		"web/core/baz.ts",
		"web/shared/bonk.ts",
		"web/shared/quux.ts",
		"web/shared/quux.generated.ts",
	}
	for i := 0; i < 100; i++ {
		// Should skip all of these directories
		paths = append(paths, fmt.Sprintf("web/node_modules/%d/deeply/nested/lib/file.ts", i))
	}

	values, err := getDirectoryContents("", paths, mockGetChildrenFunc)
	if err != nil {
		t.Fatalf("unexpected error getting directory contents: %s", err)
	}

	if diff := cmp.Diff(gitContentsOracle, values); diff != "" {
		t.Errorf("unexpected directory contents (-want +got):\n%s", diff)
	}

	expectedRequests := [][]string{
		{""},
		{"web"},
		{"web/core", "web/node_modules", "web/shared"},
		// N.B. Does not recurse into node_modules
	}
	if diff := cmp.Diff(expectedRequests, requests); diff != "" {
		t.Errorf("unexpected request to gitserver (-want +got):\n%s", diff)
	}
}

func TestGetDirectoryContents(t *testing.T) {
	gitContentsOracle := map[string][]string{
		"":                {"root/"},
		"root":            {"root/web/"},
		"root/web":        {"root/web/core/", "root/web/shared/"},
		"root/web/core":   {"root/web/core/foo.ts", "root/web/core/bar.ts", "root/web/core/baz.ts"},
		"root/web/shared": {"root/web/shared/bonk.ts", "root/web/shared/quux.ts"},
	}

	var requests [][]string
	mockGetChildrenFunc := func(dirnames []string) (map[string][]string, error) {
		out := map[string][]string{}
		for _, dirname := range dirnames {
			out[dirname] = gitContentsOracle[dirname]
		}

		requests = append(requests, dirnames)
		return out, nil
	}

	paths := []string{
		"web/core/foo.ts",
		"web/core/bar.ts",
		"web/core/baz.ts",
		"web/shared/bonk.ts",
		"web/shared/quux.ts",
		"web/shared/quux.generated.ts",
	}
	for i := 0; i < 100; i++ {
		// Should skip all of these directories
		paths = append(paths, fmt.Sprintf("web/node_modules/%d/deeply/nested/lib/file.ts", i))
	}

	values, err := getDirectoryContents("root", paths, mockGetChildrenFunc)
	if err != nil {
		t.Fatalf("unexpected error getting directory contents: %s", err)
	}

	expectedContents := map[string][]string{
		"root":            {"root/web/"},
		"root/web":        {"root/web/core/", "root/web/shared/"},
		"root/web/core":   {"root/web/core/foo.ts", "root/web/core/bar.ts", "root/web/core/baz.ts"},
		"root/web/shared": {"root/web/shared/bonk.ts", "root/web/shared/quux.ts"},
	}
	if diff := cmp.Diff(expectedContents, values); diff != "" {
		t.Errorf("unexpected directory contents (-want +got):\n%s", diff)
	}

	expectedRequests := [][]string{
		{"root"},
		{"root/web"},
		{"root/web/core", "root/web/node_modules", "root/web/shared"},
		// N.B. Does not recurse into node_modules
	}
	if diff := cmp.Diff(expectedRequests, requests); diff != "" {
		t.Errorf("unexpected request to gitserver (-want +got):\n%s", diff)
	}
}
