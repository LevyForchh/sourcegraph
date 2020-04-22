package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

func (db *dbImpl) RepoName(ctx context.Context, repositoryID int) (string, error) {
	return scanString(db.queryRow(ctx, sqlf.Sprintf(`SELECT name FROM repo WHERE id = %s`, repositoryID)))
}
