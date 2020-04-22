package db

import (
	"context"
	"database/sql"
	"errors"

	"github.com/keegancsmith/sqlf"
)

var ErrConcurrentDequeue = errors.New("record %s locked after selection")

func (db *dbImpl) Dequeue(ctx context.Context) (Upload, JobHandle, bool, error) {
	selectionQuery := `
		UPDATE lsif_uploads u SET state = 'processing', started_at = now() WHERE id = (
			SELECT id FROM lsif_uploads
			WHERE state = 'queued'
			ORDER BY uploaded_at
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING u.id
	`

	for {
		id, err := scanInt(db.queryRow(ctx, sqlf.Sprintf(selectionQuery)))
		if err != nil {
			return Upload{}, nil, false, ignoreErrNoRows(err)
		}

		upload, jobHandle, ok, err := db.dequeue(ctx, id)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}

			return Upload{}, nil, false, err
		}

		return upload, jobHandle, ok, nil
	}
}

func (db *dbImpl) dequeue(ctx context.Context, id int) (_ Upload, _ JobHandle, _ bool, err error) {
	tw, err := db.beginTx(ctx)
	if err != nil {
		return Upload{}, nil, false, err
	}
	defer func() {
		if err != nil {
			err = closeTx(tw.tx, err)
		}
	}()

	fetchQuery := `SELECT u.*, NULL FROM lsif_uploads u WHERE id = %s FOR UPDATE SKIP LOCKED LIMIT 1`
	upload, err := scanUpload(tw.queryRow(ctx, sqlf.Sprintf(fetchQuery, id)))
	if err != nil {
		return Upload{}, nil, false, err
	}

	return upload, &jobHandleImpl{ctx, tw, &txCloser{tw.tx}, id}, true, nil
}

type jobHandleImpl struct {
	ctx      context.Context
	tw       *transactionWrapper
	txCloser TxCloser
	id       int
}

func (h *jobHandleImpl) MarkComplete() (err error) {
	defer func() {
		err = h.txCloser.CloseTx(err)
	}()

	query := `
		UPDATE lsif_uploads
		SET state = 'completed', finished_at = now()
		WHERE id = %s
	`

	_, err = h.tw.exec(h.ctx, sqlf.Sprintf(query, h.id))
	return err
}

func (h *jobHandleImpl) MarkErrored(failureSummary, failureStacktrace string) (err error) {
	defer func() {
		err = h.txCloser.CloseTx(err)
	}()

	query := `
		UPDATE lsif_uploads
		SET state = 'errored', finished_at = now(), failure_summary = %s, failure_stacktrace = %s
		WHERE id = %s
	`

	_, err = h.tw.exec(h.ctx, sqlf.Sprintf(query, failureSummary, failureStacktrace, h.id))
	return err
}
