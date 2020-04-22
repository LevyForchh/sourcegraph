package db

import (
	"context"
	"testing"

	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/db/dbtesting"
)

func TestUpdatePackages(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// for foreign key relation
	insertUploads(t, db.db, Upload{ID: 42})

	if err := db.UpdatePackages(context.Background(), nil, 42, []Package{
		{Scheme: "s0", Name: "n0", Version: "v0"},
		{Scheme: "s1", Name: "n1", Version: "v1"},
		{Scheme: "s2", Name: "n2", Version: "v2"},
		{Scheme: "s3", Name: "n3", Version: "v3"},
		{Scheme: "s4", Name: "n4", Version: "v4"},
		{Scheme: "s5", Name: "n5", Version: "v5"},
		{Scheme: "s6", Name: "n6", Version: "v6"},
		{Scheme: "s7", Name: "n7", Version: "v7"},
		{Scheme: "s8", Name: "n8", Version: "v8"},
		{Scheme: "s9", Name: "n9", Version: "v9"},
	}); err != nil {
		t.Fatalf("unexpected error updating packages: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_packages"))
	if err != nil {
		t.Fatalf("unexpected error checking package count: %s", err)
	}
	if count != 10 {
		t.Errorf("unexpected package count. want=%d have=%d", 10, count)
	}
}

func TestUpdatePackagesEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	if err := db.UpdatePackages(context.Background(), nil, 42, nil); err != nil {
		t.Fatalf("unexpected error updating packages: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_packages"))
	if err != nil {
		t.Fatalf("unexpected error checking package count: %s", err)
	}
	if count != 0 {
		t.Errorf("unexpected package count. want=%d have=%d", 0, count)
	}
}

func TestUpdatePackagesWithConflicts(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// for foreign key relation
	insertUploads(t, db.db, Upload{ID: 42})

	if err := db.UpdatePackages(context.Background(), nil, 42, []Package{
		{Scheme: "s0", Name: "n0", Version: "v0"},
		{Scheme: "s1", Name: "n1", Version: "v1"},
		{Scheme: "s2", Name: "n2", Version: "v2"},
		{Scheme: "s3", Name: "n3", Version: "v3"},
	}); err != nil {
		t.Fatalf("unexpected error updating packages: %s", err)
	}

	if err := db.UpdatePackages(context.Background(), nil, 42, []Package{
		{Scheme: "s0", Name: "n0", Version: "v0"}, // duplicate
		{Scheme: "s2", Name: "n2", Version: "v2"}, // duplicate
		{Scheme: "s4", Name: "n4", Version: "v4"},
		{Scheme: "s5", Name: "n5", Version: "v5"},
		{Scheme: "s6", Name: "n6", Version: "v6"},
		{Scheme: "s7", Name: "n7", Version: "v7"},
		{Scheme: "s8", Name: "n8", Version: "v8"},
		{Scheme: "s9", Name: "n9", Version: "v9"},
	}); err != nil {
		t.Fatalf("unexpected error updating packages: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_packages"))
	if err != nil {
		t.Fatalf("unexpected error checking package count: %s", err)
	}
	if count != 10 {
		t.Errorf("unexpected package count. want=%d have=%d", 10, count)
	}
}

func TestUpdateReferences(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// for foreign key relation
	insertUploads(t, db.db, Upload{ID: 42})

	if err := db.UpdateReferences(context.Background(), nil, 42, []Reference{
		{Scheme: "s0", Name: "n0", Version: "v0", Identifiers: nil}, // TODO - filters
		{Scheme: "s1", Name: "n1", Version: "v1", Identifiers: nil}, // TODO - filters
		{Scheme: "s2", Name: "n2", Version: "v2", Identifiers: nil}, // TODO - filters
		{Scheme: "s3", Name: "n3", Version: "v3", Identifiers: nil}, // TODO - filters
		{Scheme: "s4", Name: "n4", Version: "v4", Identifiers: nil}, // TODO - filters
		{Scheme: "s5", Name: "n5", Version: "v5", Identifiers: nil}, // TODO - filters
		{Scheme: "s6", Name: "n6", Version: "v6", Identifiers: nil}, // TODO - filters
		{Scheme: "s7", Name: "n7", Version: "v7", Identifiers: nil}, // TODO - filters
		{Scheme: "s8", Name: "n8", Version: "v8", Identifiers: nil}, // TODO - filters
		{Scheme: "s9", Name: "n9", Version: "v9", Identifiers: nil}, // TODO - filters
	}); err != nil {
		t.Fatalf("unexpected error updating references: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_references"))
	if err != nil {
		t.Fatalf("unexpected error checking reference count: %s", err)
	}
	if count != 10 {
		t.Errorf("unexpected reference count. want=%d have=%d", 10, count)
	}
}

func TestUpdateReferencesEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	if err := db.UpdateReferences(context.Background(), nil, 42, nil); err != nil {
		t.Fatalf("unexpected error updating references: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_references"))
	if err != nil {
		t.Fatalf("unexpected error checking reference count: %s", err)
	}
	if count != 0 {
		t.Errorf("unexpected reference count. want=%d have=%d", 0, count)
	}
}

func TestUpdateReferencesWithDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dbtesting.SetupGlobalTestDB(t)
	db := &dbImpl{db: dbconn.Global}

	// for foreign key relation
	insertUploads(t, db.db, Upload{ID: 42})

	if err := db.UpdateReferences(context.Background(), nil, 42, []Reference{
		{Scheme: "s0", Name: "n0", Version: "v0", Identifiers: nil},
		{Scheme: "s1", Name: "n1", Version: "v1", Identifiers: nil},
		{Scheme: "s2", Name: "n2", Version: "v2", Identifiers: nil},
		{Scheme: "s3", Name: "n3", Version: "v3", Identifiers: nil},
	}); err != nil {
		t.Fatalf("unexpected error updating references: %s", err)
	}

	if err := db.UpdateReferences(context.Background(), nil, 42, []Reference{
		{Scheme: "s0", Name: "n0", Version: "v0", Identifiers: nil}, // two copies
		{Scheme: "s2", Name: "n2", Version: "v2", Identifiers: nil}, // two copies
		{Scheme: "s4", Name: "n4", Version: "v4", Identifiers: nil},
		{Scheme: "s5", Name: "n5", Version: "v5", Identifiers: nil},
		{Scheme: "s6", Name: "n6", Version: "v6", Identifiers: nil},
		{Scheme: "s7", Name: "n7", Version: "v7", Identifiers: nil},
		{Scheme: "s8", Name: "n8", Version: "v8", Identifiers: nil},
		{Scheme: "s9", Name: "n9", Version: "v9", Identifiers: nil},
	}); err != nil {
		t.Fatalf("unexpected error updating references: %s", err)
	}

	count, err := scanInt(db.db.QueryRow("SELECT COUNT(*) FROM lsif_references"))
	if err != nil {
		t.Fatalf("unexpected error checking reference count: %s", err)
	}
	if count != 12 {
		t.Errorf("unexpected reference count. want=%d have=%d", 12, count)
	}
}
