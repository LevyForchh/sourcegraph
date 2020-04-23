package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/converter"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/gitserver"
)

type WorkerOpts struct {
	DB                  db.DB
	BundleManagerClient bundles.BundleManagerClient
	PollInterval        time.Duration
}

type Worker struct {
	db                  db.DB
	bundleManagerClient bundles.BundleManagerClient
	pollInterval        time.Duration
}

func New(opts WorkerOpts) *Worker {
	return &Worker{
		db:                  opts.DB,
		bundleManagerClient: opts.BundleManagerClient,
		pollInterval:        opts.PollInterval,
	}
}

func (w *Worker) Start() error {
	for {
		if ok, err := w.dequeueAndProcess(); err != nil {
			return err
		} else if !ok {
			time.Sleep(w.pollInterval)
		}
	}
}

func (w *Worker) dequeueAndProcess() (bool, error) {
	upload, jobHandle, ok, err := w.db.Dequeue(context.Background())
	if err != nil || !ok {
		return false, err
	}

	err = w.process(context.Background(), upload, jobHandle)
	if err != nil {
		if markErr := jobHandle.MarkErrored(err.Error(), "TODO"); markErr != nil {
			err = multierror.Append(err, markErr)
		}
	}
	if err := jobHandle.CloseTx(err); err != nil {
		fmt.Printf("WHOOPSIE: %s\n", err)
	}

	return true, nil
}

func (w *Worker) process(ctx context.Context, upload db.Upload, jobHandle db.JobHandle) (err error) {
	name, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(name); err != nil {
			// TODO - warn
		}
	}()

	filename, err := w.bundleManagerClient.GetUpload(ctx, upload.ID, name)
	if err != nil {
		return err
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	newFilename := filepath.Join(name, uuid.String())

	fn := func(dirnames []string) (map[string][]string, error) {
		return gitserver.DirectoryChildren(w.db, upload.RepositoryID, upload.Commit, dirnames)
	}

	// packages, refs
	packages, references, err2 := converter.Convert(fn, upload.Root, filename, newFilename)
	if err2 != nil {
		return err2
	}

	// TODO - fix type differences
	// TODO- use same txn
	// if err := w.db.UpdatePackages(context.Background(), nil, upload.ID, packages); err != nil {
	// 	return err
	// }
	// if err := w.db.UpdateReferences(context.Background(), nil, upload.ID, references); err != nil {
	// 	return err
	// }
	fmt.Printf("%d %d\n", len(packages), len(references))

	f, err := os.Open(newFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := w.bundleManagerClient.SendDB(ctx, upload.ID, f); err != nil {
		return err
	}

	// TODO - do everything below here in a nested transaction so we can
	// mark it as failed without deleting dumps or updating visibility
	// on a late processing failure

	if err := w.db.DeleteOverlappingDumps(ctx, jobHandle.Tx(), upload.RepositoryID, upload.Commit, upload.Root, upload.Indexer); err != nil {
		return err
	}

	if err := jobHandle.MarkComplete(); err != nil {
		return err
	}

	if err := w.updateCommits(ctx, jobHandle.Tx(), upload.RepositoryID, upload.Commit); err != nil {
		return err
	}

	return nil
}

func (w *Worker) updateCommits(ctx context.Context, tx *sql.Tx, repositoryID int, commit string) error {
	tipCommit, err := gitserver.Head(w.db, repositoryID)
	if err != nil {
		return err
	}

	newCommits, err := gitserver.CommitsNear(w.db, repositoryID, tipCommit)
	if err != nil {
		return err
	}

	if tipCommit != commit {
		// If the tip is ahead of this commit, we also want to discover all of
		// the commits between this commit and the tip so that we can accurately
		// determine what is visible from the tip. If we do not do this before the
		// updateDumpsVisibleFromTip call below, no dumps will be reachable from
		// the tip and all dumps will be invisible.
		additionalCommits, err := gitserver.CommitsNear(w.db, repositoryID, commit)
		if err != nil {
			return err
		}

		for k, vs := range additionalCommits {
			newCommits[k] = append(newCommits[k], vs...)
		}
	}

	// TODO - need to do same discover on query
	// TODO - determine if we know about these commits first
	if err := w.db.UpdateCommits(ctx, tx, repositoryID, newCommits); err != nil {
		return err
	}

	if err := w.db.UpdateDumpsVisibleFromTip(ctx, tx, repositoryID, tipCommit); err != nil {
		return err
	}

	return nil
}
