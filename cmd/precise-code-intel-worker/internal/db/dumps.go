package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

//
// TODO - better thing than this TW (at least make an interface for it)

func (db *dbImpl) DeleteOverlappingDumps(ctx context.Context, tw *transactionWrapper, repositoryID int, commit, root, indexer string) (err error) {
	if tw == nil {
		tw, err = db.beginTx(ctx)
		if err != nil {
			return err
		}
		defer func() {
			err = closeTx(tw.tx, err)
		}()
	}

	query := `
		DELETE from lsif_uploads
		WHERE repository_id = %d AND commit = %s AND root = %s AND indexer = %s AND state = 'completed'
	`

	_, err = tw.exec(ctx, sqlf.Sprintf(query, repositoryID, commit, root, indexer))
	return err
}
