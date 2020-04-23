package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/keegancsmith/sqlf"
)

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

	jobHandle := &jobHandleImpl{
		ctx:      ctx,
		id:       id,
		tw:       tw,
		txCloser: &txCloser{tw.tx},
	}

	return upload, jobHandle, true, nil
}

type jobHandleImpl struct {
	ctx        context.Context
	id         int
	tw         *transactionWrapper
	txCloser   TxCloser
	marked     bool
	savepoints []string
}

var _ JobHandle = &jobHandleImpl{}

func (h *jobHandleImpl) CloseTx(err error) error {
	if err == nil && !h.marked {
		err = fmt.Errorf("job not finalized")
	}

	return h.txCloser.CloseTx(err)
}

func (h *jobHandleImpl) Tx() *sql.Tx {
	return h.tw.tx
}

func (h *jobHandleImpl) Savepoint() error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	savepointID := strings.ReplaceAll(id.String(), "-", "_")
	h.savepoints = append(h.savepoints, savepointID)
	_, err = h.tw.exec(h.ctx, sqlf.Sprintf(`SAVEPOINT %s`, savepointID))
	return err
}

func (h *jobHandleImpl) RollbackToLastSavepoint() error {
	if n := len(h.savepoints); n > 0 {
		var savepointID string
		savepointID, h.savepoints = h.savepoints[n-1], h.savepoints[:n-1]
		_, err := h.tw.exec(h.ctx, sqlf.Sprintf(`ROLLBACK TO SAVEPOINT %s`, savepointID))
		return err
	}

	return fmt.Errorf("no savepoint defined")
}

func (h *jobHandleImpl) MarkComplete() (err error) {
	query := `
		UPDATE lsif_uploads
		SET state = 'completed', finished_at = now()
		WHERE id = %s
	`

	h.marked = true
	_, err = h.tw.exec(h.ctx, sqlf.Sprintf(query, h.id))
	return err
}

func (h *jobHandleImpl) MarkErrored(failureSummary, failureStacktrace string) (err error) {
	query := `
		UPDATE lsif_uploads
		SET state = 'errored', finished_at = now(), failure_summary = %s, failure_stacktrace = %s
		WHERE id = %s
	`

	h.marked = true
	_, err = h.tw.exec(h.ctx, sqlf.Sprintf(query, failureSummary, failureStacktrace, h.id))
	return err
}
