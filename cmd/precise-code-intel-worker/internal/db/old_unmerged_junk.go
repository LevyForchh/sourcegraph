package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/keegancsmith/sqlf"
	"github.com/sourcegraph/sourcegraph/internal/db/dbutil"
)

// This is stuff already defined in the API that's not merged yet

type dbImpl struct {
	db *sql.DB
}

var _ DB = &dbImpl{}

// GetUploadByID returns an upload by its identifier and boolean flag indicating its existence.
func (db *dbImpl) GetUploadByID(ctx context.Context, id int) (Upload, bool, error) {
	query := `
		SELECT
			u.id,
			u.commit,
			u.root,
			u.visible_at_tip,
			u.uploaded_at,
			u.state,
			u.failure_summary,
			u.failure_stacktrace,
			u.started_at,
			u.finished_at,
			u.tracing_context,
			u.repository_id,
			u.indexer,
			s.rank
		FROM lsif_uploads u
		LEFT JOIN (
			SELECT r.id, RANK() OVER (ORDER BY r.uploaded_at) as rank
			FROM lsif_uploads r
			WHERE r.state = 'queued'
		) s
		ON u.id = s.id
		WHERE u.id = %s
	`

	upload, err := scanUpload(db.queryRow(ctx, sqlf.Sprintf(query, id)))
	if err != nil {
		return Upload{}, false, ignoreErrNoRows(err)
	}

	return upload, true, nil
}

// Dump is a subset of the lsif_uploads table (queried via the lsif_dumps view) and stores
// only processed records.
type Dump struct {
	ID                int        `json:"id"`
	Commit            string     `json:"commit"`
	Root              string     `json:"root"`
	VisibleAtTip      bool       `json:"visibleAtTip"`
	UploadedAt        time.Time  `json:"uploadedAt"`
	State             string     `json:"state"`
	FailureSummary    *string    `json:"failureSummary"`
	FailureStacktrace *string    `json:"failureStacktrace"`
	StartedAt         *time.Time `json:"startedAt"`
	FinishedAt        *time.Time `json:"finishedAt"`
	TracingContext    string     `json:"tracingContext"`
	RepositoryID      int        `json:"repositoryId"`
	Indexer           string     `json:"indexer"`
}

// scanDump populates a Dump value from the given scanner.
func scanDump(scanner Scanner) (dump Dump, err error) {
	err = scanner.Scan(
		&dump.ID,
		&dump.Commit,
		&dump.Root,
		&dump.VisibleAtTip,
		&dump.UploadedAt,
		&dump.State,
		&dump.FailureSummary,
		&dump.FailureStacktrace,
		&dump.StartedAt,
		&dump.FinishedAt,
		&dump.TracingContext,
		&dump.RepositoryID,
		&dump.Indexer,
	)
	return
}

// insertUploads populates the lsif_uploads table with the given upload models.
func insertUploads(t *testing.T, db *sql.DB, uploads ...Upload) {
	for _, upload := range uploads {
		if upload.Commit == "" {
			upload.Commit = makeCommit(upload.ID)
		}
		if upload.State == "" {
			upload.State = "completed"
		}
		if upload.RepositoryID == 0 {
			upload.RepositoryID = 50
		}
		if upload.Indexer == "" {
			upload.Indexer = "lsif-go"
		}

		query := sqlf.Sprintf(`
			INSERT INTO lsif_uploads (
				id,
				commit,
				root,
				visible_at_tip,
				uploaded_at,
				state,
				failure_summary,
				failure_stacktrace,
				started_at,
				finished_at,
				tracing_context,
				repository_id,
				indexer
			) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		`,
			upload.ID,
			upload.Commit,
			upload.Root,
			upload.VisibleAtTip,
			upload.UploadedAt,
			upload.State,
			upload.FailureSummary,
			upload.FailureStacktrace,
			upload.StartedAt,
			upload.FinishedAt,
			upload.TracingContext,
			upload.RepositoryID,
			upload.Indexer,
		)

		if _, err := db.ExecContext(context.Background(), query.Query(sqlf.PostgresBindVar), query.Args()...); err != nil {
			t.Fatalf("unexpected error while inserting dump: %s", err)
		}
	}
}

func makeCommit(i int) string {
	return fmt.Sprintf("%040d", i)
}

// GetDumpByID returns a dump by its identifier and boolean flag indicating its existence.
func (db *dbImpl) GetDumpByID(ctx context.Context, id int) (Dump, bool, error) {
	query := `
		SELECT
			d.id,
			d.commit,
			d.root,
			d.visible_at_tip,
			d.uploaded_at,
			d.state,
			d.failure_summary,
			d.failure_stacktrace,
			d.started_at,
			d.finished_at,
			d.tracing_context,
			d.repository_id,
			d.indexer
		FROM lsif_dumps d WHERE id = %d
	`

	dump, err := scanDump(db.queryRow(ctx, sqlf.Sprintf(query, id)))
	if err != nil {
		return Dump{}, false, ignoreErrNoRows(err)
	}

	return dump, true, nil
}

// New creates a new instance of DB connected to the given Postgres DSN.
func New(postgresDSN string) (DB, error) {
	db, err := dbutil.NewDB(postgresDSN, "precise-code-intel-api-server")
	if err != nil {
		return nil, err
	}

	return &dbImpl{db: db}, nil
}

// query performs Query on the underlying connection.
func (db *dbImpl) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// queryRow performs QueryRow on the underlying connection.
func (db *dbImpl) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return db.db.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// beginTx performs BeginTx on the underlying connection and wraps the transaction.
func (db *dbImpl) beginTx(ctx context.Context) (*transactionWrapper, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &transactionWrapper{tx}, nil
}

// TxCloser is a convenience wrapper for closing SQL transactions.
type TxCloser interface {
	// CloseTx commits the transaction on a nil error value and performs a rollback
	// otherwise. If an error occurs during commit or rollback of the transaction,
	// the error is added to the resulting error value.
	CloseTx(err error) error
}

type txCloser struct {
	tx *sql.Tx
}

func (txc *txCloser) CloseTx(err error) error {
	return closeTx(txc.tx, err)
}

func closeTx(tx *sql.Tx, err error) error {
	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			err = multierror.Append(err, rollErr)
		}
		return err
	}

	return tx.Commit()
}

// Upload is a subset of the lsif_uploads table and stores both processed and unprocessed
// records.
type Upload struct {
	ID                int        `json:"id"`
	Commit            string     `json:"commit"`
	Root              string     `json:"root"`
	VisibleAtTip      bool       `json:"visibleAtTip"`
	UploadedAt        time.Time  `json:"uploadedAt"`
	State             string     `json:"state"`
	FailureSummary    *string    `json:"failureSummary"`
	FailureStacktrace *string    `json:"failureStacktrace"`
	StartedAt         *time.Time `json:"startedAt"`
	FinishedAt        *time.Time `json:"finishedAt"`
	TracingContext    string     `json:"tracingContext"`
	RepositoryID      int        `json:"repositoryId"`
	Indexer           string     `json:"indexer"`
	Rank              *int       `json:"placeInQueue"`
}

// Scanner is the common interface shared by *sql.Row and *sql.Rows.
type Scanner interface {
	// Scan copies the values of the current row into the values pointed at by dest.
	Scan(dest ...interface{}) error
}

// scanUpload populates an Upload value from the given scanner.
func scanUpload(scanner Scanner) (upload Upload, err error) {
	err = scanner.Scan(
		&upload.ID,
		&upload.Commit,
		&upload.Root,
		&upload.VisibleAtTip,
		&upload.UploadedAt,
		&upload.State,
		&upload.FailureSummary,
		&upload.FailureStacktrace,
		&upload.StartedAt,
		&upload.FinishedAt,
		&upload.TracingContext,
		&upload.RepositoryID,
		&upload.Indexer,
		&upload.Rank,
	)
	return
}

// ignoreErrNoRows returns the given error if it's not sql.ErrNoRows.
func ignoreErrNoRows(err error) error {
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

// scanInt populates an integer value from the given scanner.
func scanInt(scanner Scanner) (value int, err error) {
	err = scanner.Scan(&value)
	return
}

type transactionWrapper struct {
	tx *sql.Tx
}

// query performs QueryContext on the underlying transaction.
func (tw *transactionWrapper) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return tw.tx.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// queryRow performs QueryRow on the underlying transaction.
func (tw *transactionWrapper) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return tw.tx.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// exec performs Exec on the underlying transaction.
func (tw *transactionWrapper) exec(ctx context.Context, query *sqlf.Query) (sql.Result, error) {
	return tw.tx.ExecContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}
