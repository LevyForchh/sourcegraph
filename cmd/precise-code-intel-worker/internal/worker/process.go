package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	bundles "github.com/sourcegraph/sourcegraph/internal/codeintel/bundles/client"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/db"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/gitserver"
)

func process(ctx context.Context, db db.DB, bundleManagerClient bundles.BundleManagerClient,
	upload db.Upload, jobHandle db.JobHandle) (err error) {
	name, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(name); err != nil {
			// TODO - warn
		}
	}()

	filename, err := bundleManagerClient.GetUpload(ctx, upload.ID, name)
	if err != nil {
		return err
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	newFilename := filepath.Join(name, uuid.String())

	fn := func(dirnames []string) (map[string][]string, error) {
		return gitserver.DirectoryChildren(db, upload.RepositoryID, upload.Commit, dirnames)
	}

	// packages, refs
	packages, references, err2 := convert(fn, upload.Root, filename, newFilename)
	if err2 != nil {
		return err2
	}

	// TODO - fix type differences
	// TODO- use same txn
	// if err := db.UpdatePackages(context.Background(), nil, upload.ID, packages); err != nil {
	// 	return err
	// }
	// if err := db.UpdateReferences(context.Background(), nil, upload.ID, references); err != nil {
	// 	return err
	// }
	fmt.Printf("%d %d\n", len(packages), len(references))

	f, err := os.Open(newFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := bundleManagerClient.SendDB(ctx, upload.ID, f); err != nil {
		return err
	}

	if err := jobHandle.Savepoint(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rollbackErr := jobHandle.RollbackToLastSavepoint(); rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		}
	}()

	if err := db.DeleteOverlappingDumps(ctx, jobHandle.Tx(), upload.RepositoryID, upload.Commit, upload.Root, upload.Indexer); err != nil {
		return err
	}

	if err := jobHandle.MarkComplete(); err != nil {
		return err
	}

	if err := updateCommits(ctx, db, jobHandle.Tx(), upload.RepositoryID, upload.Commit); err != nil {
		return err
	}

	return nil
}

func updateCommits(ctx context.Context, db db.DB, tx *sql.Tx, repositoryID int, commit string) error {
	tipCommit, err := gitserver.Head(db, repositoryID)
	if err != nil {
		return err
	}

	newCommits, err := gitserver.CommitsNear(db, repositoryID, tipCommit)
	if err != nil {
		return err
	}

	if tipCommit != commit {
		// If the tip is ahead of this commit, we also want to discover all of
		// the commits between this commit and the tip so that we can accurately
		// determine what is visible from the tip. If we do not do this before the
		// updateDumpsVisibleFromTip call below, no dumps will be reachable from
		// the tip and all dumps will be invisible.
		additionalCommits, err := gitserver.CommitsNear(db, repositoryID, commit)
		if err != nil {
			return err
		}

		for k, vs := range additionalCommits {
			newCommits[k] = append(newCommits[k], vs...)
		}
	}

	// TODO - need to do same discover on query
	// TODO - determine if we know about these commits first
	if err := db.UpdateCommits(ctx, tx, repositoryID, newCommits); err != nil {
		return err
	}

	if err := db.UpdateDumpsVisibleFromTip(ctx, tx, repositoryID, tipCommit); err != nil {
		return err
	}

	return nil
}
