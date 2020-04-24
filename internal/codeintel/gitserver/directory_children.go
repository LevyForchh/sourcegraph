package gitserver

import (
	"bytes"
	"context"
	"strings"

	"github.com/sourcegraph/sourcegraph/internal/api"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/db"
	"github.com/sourcegraph/sourcegraph/internal/gitserver"
)

func DirectoryChildren(db db.DB, repositoryID int, commit string, dirnames []string) (map[string][]string, error) {
	repoName, err := db.RepoName(context.Background(), repositoryID)
	if err != nil {
		return nil, err
	}

	cmd := gitserver.DefaultClient.Command("git", append([]string{"ls-tree", "--name-only", commit, "--"}, cleanDirectoriesForLsTree(dirnames)...)...)
	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
	out, err := cmd.CombinedOutput(context.Background())
	if err != nil {
		return nil, err
	}

	return parseDirectoryChildren(dirnames, strings.Split(string(bytes.TrimSpace(out)), "\n")), nil
}

func cleanDirectoriesForLsTree(dirnames []string) []string {
	var args []string
	for _, dir := range dirnames {
		if dir == "" {
			args = append(args, ".")
		} else {
			if !strings.HasSuffix(dir, "/") {
				dir += "/"
			}
			args = append(args, dir)
		}
	}

	return args
}

func parseDirectoryChildren(dirnames []string, paths []string) map[string][]string {
	childrenMap := map[string][]string{}
	for _, dir := range dirnames {
		if dir == "" {
			var children []string
			for _, path := range paths {
				if !strings.Contains(path, "/") {
					children = append(children, path)
				}
			}

			childrenMap[dir] = children
		} else {
			var children []string
			for _, path := range paths {
				if strings.HasPrefix(path, dir) {
					children = append(children, path)
				}
			}

			childrenMap[dir] = children
		}
	}

	return childrenMap
}
