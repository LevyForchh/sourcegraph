package commits

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
	"github.com/sourcegraph/sourcegraph/internal/api"
	"github.com/sourcegraph/sourcegraph/internal/gitserver"
)

func Head(db db.DB, repositoryID int) (string, error) {
	// TODO
	repoName, err := db.RepoName(context.Background(), repositoryID)
	if err != nil {
		return "", err
	}

	cmd := gitserver.DefaultClient.Command("git", "rev-parse", "HEAD")
	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
	out, err := cmd.CombinedOutput(context.Background())
	if err != nil {
		return "", err
	}

	return string(bytes.TrimSpace(out)), nil
}

func CommitsNear(db db.DB, repositoryID int, commit string) (map[string][]string, error) {
	// TODO
	repoName, err := db.RepoName(context.Background(), repositoryID)
	if err != nil {
		return nil, err
	}

	// TODO - move
	const MaxCommitsPerUpdate = 150 // MaxTraversalLimit * 1.5

	cmd := gitserver.DefaultClient.Command("git", "log", "--pretty=%H %P", commit, fmt.Sprintf("-%d", MaxCommitsPerUpdate))
	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
	out, err := cmd.CombinedOutput(context.Background())
	if err != nil {
		return nil, err
	}

	return parseCommitsNear(strings.Split(string(bytes.TrimSpace(out)), "\n")), nil
}

func parseCommitsNear(allDudes []string) map[string][]string {
	commits := map[string][]string{}

	for _, dude := range allDudes {
		line := strings.TrimSpace(dude)
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		commits[parts[0]] = parts[1:]
	}

	return commits
}
