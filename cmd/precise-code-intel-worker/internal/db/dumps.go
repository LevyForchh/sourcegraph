package db

import (
	"context"
	"database/sql"

	"github.com/keegancsmith/sqlf"
)

//
// TODO - better thing than this TW (at least make an interface for it)

func (db *dbImpl) DeleteOverlappingDumps(ctx context.Context, tx *sql.Tx, repositoryID int, commit, root, indexer string) (err error) {
	if tx == nil {
		tx, err = db.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			err = closeTx(tx, err)
		}()
	}
	tw := &transactionWrapper{tx}

	query := `
		DELETE from lsif_uploads
		WHERE repository_id = %d AND commit = %s AND root = %s AND indexer = %s AND state = 'completed'
	`

	_, err = tw.exec(ctx, sqlf.Sprintf(query, repositoryID, commit, root, indexer))
	return err
}
