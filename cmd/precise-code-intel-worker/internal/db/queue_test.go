package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

func TestDequeueConversionSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Add dequeueable upload
	insertUploads(t, db.db, Upload{ID: 1, State: "queued"})

	upload, jobHandle, ok, err := db.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("unexpected error dequeueing upload: %s", err)
	}
	if !ok {
		t.Fatalf("expected something to be dequeueable")
	}

	if upload.ID != 1 {
		t.Errorf("unexpected upload id. want=%d have=%d", 1, upload.ID)
	}
	if upload.State != "processing" {
		t.Errorf("unexpected state. want=%s have=%s", "processing", upload.State)
	}

	if state, err := scanString(db.db.QueryRow("SELECT state FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting state: %s", err)
	} else if state != "processing" {
		t.Errorf("unexpected state outside of txn. want=%s have=%s", "processing", state)
	}

	if err := jobHandle.MarkComplete(); err != nil {
		t.Fatalf("unexpected error marking upload complete: %s", err)
	}
	if err := jobHandle.CloseTx(nil); err != nil {
		t.Fatalf("unexpected error closing transaction: %s", err)
	}

	if state, err := scanString(db.db.QueryRow("SELECT state FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting state: %s", err)
	} else if state != "completed" {
		t.Errorf("unexpected state outside of txn. want=%s have=%s", "completed", state)
	}
}

func TestDequeueConversionError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Add dequeueable upload
	insertUploads(t, db.db, Upload{ID: 1, State: "queued"})

	upload, jobHandle, ok, err := db.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("unexpected error dequeueing upload: %s", err)
	}
	if !ok {
		t.Fatalf("expected something to be dequeueable")
	}

	if upload.ID != 1 {
		t.Errorf("unexpected upload id. want=%d have=%d", 1, upload.ID)
	}
	if upload.State != "processing" {
		t.Errorf("unexpected state. want=%s have=%s", "processing", upload.State)
	}

	if state, err := scanString(db.db.QueryRow("SELECT state FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting state: %s", err)
	} else if state != "processing" {
		t.Errorf("unexpected state outside of txn. want=%s have=%s", "processing", state)
	}

	if err := jobHandle.MarkErrored("test summary", "test stacktrace"); err != nil {
		t.Fatalf("unexpected error marking upload complete: %s", err)
	}
	if err := jobHandle.CloseTx(nil); err != nil {
		t.Fatalf("unexpected error closing transaction: %s", err)
	}

	if state, err := scanString(db.db.QueryRow("SELECT state FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting state: %s", err)
	} else if state != "errored" {
		t.Errorf("unexpected state outside of txn. want=%s have=%s", "errored", state)
	}

	if summary, err := scanString(db.db.QueryRow("SELECT failure_summary FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting failure_summary: %s", err)
	} else if summary != "test summary" {
		t.Errorf("unexpected failure summary outside of txn. want=%s have=%s", "test summary", summary)
	}

	if stacktrace, err := scanString(db.db.QueryRow("SELECT failure_stacktrace FROM lsif_uploads WHERE id = 1")); err != nil {
		t.Errorf("unexpected error getting failure_stacktrace: %s", err)
	} else if stacktrace != "test stacktrace" {
		t.Errorf("unexpected failure stacktrace outside of txn. want=%s have=%s", "test stacktrace", stacktrace)
	}
}

func TestDequeueSkipsLocked(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	t1 := time.Now().UTC()
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)
	insertUploads(
		t,
		db.db,
		Upload{ID: 1, State: "queued", UploadedAt: t1},
		Upload{ID: 2, State: "processing", UploadedAt: t2},
		Upload{ID: 3, State: "queued", UploadedAt: t3},
	)

	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Row lock upload 1 in a transaction which should be skipped by ResetStalled
	if _, err := tx.Query(`SELECT * FROM lsif_uploads WHERE id = 1 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	upload, jobHandle, ok, err := db.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("unexpected error dequeueing upload: %s", err)
	}
	if !ok {
		t.Fatalf("expected something to be dequeueable")
	}
	defer func() { _ = jobHandle.CloseTx(nil) }()

	if upload.ID != 3 {
		t.Errorf("unexpected upload id. want=%d have=%d", 3, upload.ID)
	}
	if upload.State != "processing" {
		t.Errorf("unexpected state. want=%s have=%s", "processing", upload.State)
	}
}

func TestDequeueEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	_, _, ok, err := db.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("unexpected error dequeueing upload: %s", err)
	}
	if ok {
		t.Fatalf("unexpected dequeue")
	}
}

func TestDequeueConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// Add dequeueable upload
	insertUploads(t, db.db, Upload{ID: 1, State: "queued"})

	_, closer1, ok1, err1 := db.dequeue(context.Background(), 1)
	_, closer2, ok2, err2 := db.dequeue(context.Background(), 1)

	if err1 != sql.ErrNoRows && err2 != sql.ErrNoRows {
		t.Errorf("expected one error to be sql.ErrNoRows. have=%q and %q", err1, err2)
	}

	if ok1 {
		_ = closer1.CloseTx(nil)
	}
	if ok2 {
		_ = closer2.CloseTx(nil)
	}
}
