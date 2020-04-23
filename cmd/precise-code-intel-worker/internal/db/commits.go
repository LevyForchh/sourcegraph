package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

func (db *dbImpl) UpdateCommits(ctx context.Context, repositoryID int, commits map[string][]string) error {
	var rows []*sqlf.Query
	for commit, parents := range commits {
		for _, parent := range parents {
			rows = append(rows, sqlf.Sprintf("(%d, %s, %s)", repositoryID, commit, parent))
		}

		if len(parents) == 0 {
			rows = append(rows, sqlf.Sprintf("(%d, %s, NULL)", repositoryID, commit))
		}
	}

	// TODO - test conflict
	query := `INSERT INTO lsif_commits (repository_id, "commit", parent_commit) VALUES %s ON CONFLICT DO NOTHING`
	if err := db.exec(ctx, sqlf.Sprintf(query, sqlf.Join(rows, ","))); err != nil {
		return err
	}

	return nil
}
