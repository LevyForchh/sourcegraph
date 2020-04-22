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

// updateDumpsVisibleFromTip recalculates the visible_at_tip flag of all dumps of the given repository.
func (db *dbImpl) updateDumpsVisibleFromTip(ctx context.Context, tw *transactionWrapper, repositoryID int, tipCommit string) (err error) {
	if tw == nil {
		tw, err = db.beginTx(ctx)
		if err != nil {
			return err
		}
		defer func() {
			err = closeTx(tw.tx, err)
		}()
	}

	// Update dump records by:
	//   (1) unsetting the visibility flag of all previously visible dumps, and
	//   (2) setting the visibility flag of all currently visible dumps
	query := `
		UPDATE lsif_dumps d
		SET visible_at_tip = id IN (SELECT * from visible_ids)
		WHERE d.repository_id = %s AND (d.id IN (SELECT * from visible_ids) OR d.visible_at_tip)
	`

	_, err = tw.exec(ctx, withAncestorLineage(query, repositoryID, tipCommit, repositoryID))
	return err
}

// MaxTraversalLimit is the maximum size of the CTE result set when traversing commit ancestor
// and descendants. This value affects how stale an upload can be while still serving code
// intelligence for a nearby commit.
const MaxTraversalLimit = 100

// visibleIDsCTE defines a CTE `visible_ids` that returns an ordered list of dump identifiers
// given a previously defined CTE `lineage`. The dump identifiers returned exclude the dumps
// shadowed by another dump: one dump shadows another when it has the same indexer value, has
// a root value enclosing the other, and when it is at a commit closer to the target commit
// value.
var visibleIDsCTE = `
	-- Limit the visibility to the maximum traversal depth and approximate
	-- each commit's depth by its row number.
	limited_lineage AS (
		SELECT a.*, row_number() OVER() as n from lineage a LIMIT ` + fmt.Sprintf("%d", MaxTraversalLimit) + `
	),
	-- Correlate commits to dumps and filter out commits without LSIF data
	lineage_with_dumps AS (
		SELECT a.*, d.root, d.indexer, d.id as dump_id FROM limited_lineage a
		JOIN lsif_dumps d ON d.repository_id = a.repository_id AND d."commit" = a."commit"
	),
	visible_ids AS (
		-- Remove dumps where there exists another visible dump of smaller depth with an
		-- overlapping root from the same indexer. Such dumps would not be returned with
		-- a closest commit query so we don't want to return results for them in global
		-- find-reference queries either.
		SELECT DISTINCT t1.dump_id as id FROM lineage_with_dumps t1 WHERE NOT EXISTS (
			SELECT 1 FROM lineage_with_dumps t2
			WHERE t2.n < t1.n AND t1.indexer = t2.indexer AND (
				t2.root LIKE (t1.root || '%%%%') OR
				t1.root LIKE (t2.root || '%%%%')
			)
		)
	)
`

// withAncestorLineage prepares the given query by defining the CTE `visible_ids`. The set of
// candidate dumps are chosen by tracing the commit graph backwards (towards ancestors).
func withAncestorLineage(query string, repositoryID int, commit string, args ...interface{}) *sqlf.Query {
	queryWithCTEs := `
		WITH
		RECURSIVE lineage(id, "commit", parent, repository_id) AS (
			SELECT c.* FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
			UNION
			SELECT c.* FROM lineage a JOIN lsif_commits c ON a.repository_id = c.repository_id AND a.parent = c."commit"
		), ` + visibleIDsCTE + " " + query

	return sqlf.Sprintf(queryWithCTEs, append([]interface{}{repositoryID, commit}, args...)...)
}

// withBidirectionalLineage prepares the given query by defining the CTE `visible_ids`. The set of
// candidatedumps are chosen by tracing the commit graph both forwards and backwards. The resulting
// order of dumps are interleaved such that two dumps with a similar "distance" are near eachother
// in the result set. This prevents the resulting dumps from preferring one direction over the other.
func withBidirectionalLineage(query string, repositoryID int, commit string, args ...interface{}) *sqlf.Query {
	queryWithCTEs := `
		WITH
		RECURSIVE lineage(id, "commit", parent_commit, repository_id, direction) AS (
			SELECT l.* FROM (
				-- seed recursive set with commit looking in ancestor direction
				SELECT c.*, 'A' FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
				UNION
				-- seed recursive set with commit looking in descendant direction
				SELECT c.*, 'D' FROM lsif_commits c WHERE c.repository_id = %s AND c."commit" = %s
			) l
			UNION
			SELECT * FROM (
				WITH l_inner AS (SELECT * FROM lineage)
				-- get next ancestors (multiple parents for merge commits)
				SELECT c.*, 'A' FROM l_inner l JOIN lsif_commits c ON l.direction = 'A' AND c.repository_id = l.repository_id AND c."commit" = l.parent_commit
				UNION
				-- get next descendants
				SELECT c.*, 'D' FROM l_inner l JOIN lsif_commits c ON l.direction = 'D' and c.repository_id = l.repository_id AND c.parent_commit = l."commit"
			) subquery
		), ` + visibleIDsCTE + " " + query

	return sqlf.Sprintf(queryWithCTEs, append([]interface{}{repositoryID, commit, repositoryID, commit}, args...)...)
}
