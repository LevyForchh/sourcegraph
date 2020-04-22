package existence

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
	"github.com/sourcegraph/sourcegraph/internal/api"
	"github.com/sourcegraph/sourcegraph/internal/gitserver"
)

func getDirectoryChildren(db db.DB, repositoryID int, commit string, dirnames []string) (map[string][]string, error) {
	args := []string{"ls-tree", "--name-only", commit, "--"}
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

	repoName, err := db.RepoName(context.Background(), repositoryID)
	if err != nil {
		return nil, err
	}

	cmd := gitserver.DefaultClient.Command("git", args...)
	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
	out, err := cmd.CombinedOutput(context.Background())
	if err != nil {
		return nil, err
	}

	childrenMap := map[string][]string{}
	allDudes := strings.Split(string(bytes.TrimSpace(out)), "\n")

	for _, dir := range dirnames {
		if dir == "" {
			var children []string
			for _, dude := range allDudes {
				if !strings.Contains(dude, "/") {
					children = append(children, dude)
				}
			}

			childrenMap[dir] = children
		} else {
			var children []string
			for _, dude := range allDudes {
				if strings.HasPrefix(dude, dir) {
					children = append(children, dude)
				}
			}

			childrenMap[dir] = children
		}
	}

	return childrenMap, nil
}

//
// TODO - move this to another guy

func getCommitsNear(db db.DB, repositoryID int, commit string) (map[string][]string, error) {
	// TODO
	repoName, err := db.RepoName(context.Background(), repositoryID)
	if err != nil {
		return nil, err
	}

	// TODO - move
	const MaxCommitsPerUpdate = 150 // MAX_TRAVERSAL_LIMIT * 1.5

	cmd := gitserver.DefaultClient.Command("git", "log", "--pretty=%H %P", commit, fmt.Sprintf("-%d", MaxCommitsPerUpdate))
	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
	out, err := cmd.CombinedOutput(context.Background())
	if err != nil {
		return nil, err
	}

	allDudes := strings.Split(string(bytes.TrimSpace(out)), "\n")
	commits := map[string][]string{}

	for _, dude := range allDudes {
		line := strings.TrimSpace(dude)
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		commits[parts[0]] = parts[1:]
	}

	return commits, nil
}
