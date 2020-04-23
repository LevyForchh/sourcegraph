package db

import (
	"context"
	"database/sql"

	"github.com/keegancsmith/sqlf"
)

type JobHandle interface {
	TxCloser

	Tx() *sql.Tx
	Savepoint() error
	RollbackToLastSavepoint() error
	MarkComplete() error
	MarkErrored(failureSummary, failureStacktrace string) error
}

type DB interface {
	RepoName(ctx context.Context, repositoryID int) (string, error)
	Dequeue(ctx context.Context) (Upload, JobHandle, bool, error)
	UpdatePackages(ctx context.Context, tx *sql.Tx, uploadID int, packages []Package) error
	UpdateReferences(ctx context.Context, tx *sql.Tx, uploadID int, references []Reference) error
	UpdateCommits(ctx context.Context, tx *sql.Tx, repositoryID int, commits map[string][]string) error
	DeleteOverlappingDumps(ctx context.Context, tx *sql.Tx, repositoryID int, commit, root, indexer string) error
	UpdateDumpsVisibleFromTip(ctx context.Context, tx *sql.Tx, repositoryID int, tipCommit string) error
}

func (db *dbImpl) exec(ctx context.Context, query *sqlf.Query) error {
	_, err := db.db.ExecContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
	return err
}

// scanString populates an string value from the given scanner.
func scanString(scanner Scanner) (value string, err error) {
	err = scanner.Scan(&value)
	return value, err
}

func (db *dbImpl) UpdateDumpsVisibleFromTip(ctx context.Context, tx *sql.Tx, repositoryID int, tipCommit string) error {
	return db.updateDumpsVisibleFromTip(ctx, &transactionWrapper{tx}, repositoryID, tipCommit)
}
