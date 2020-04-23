package worker

import (
	"context"
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
		fmt.Printf("BUMPIN\n")

		upload, jobHandle, ok, err := w.db.Dequeue(context.Background())
		if err != nil {
			return err
		}
		if !ok {
			time.Sleep(w.pollInterval)
			continue
		}

		fmt.Printf("Processing\n")
		if err := w.process(upload, jobHandle); err != nil {
			fmt.Printf("FAILED TO PROCESS: %#v\n", err)
			return err
		}

		fmt.Printf("PROCESSED!\n")
	}
}

func (w *Worker) process(upload db.Upload, jobHandle db.JobHandle) (err error) {
	defer func() {
		if err != nil {
			// TODO- extract text
			if markErr := jobHandle.MarkErrored("", ""); markErr != nil {
				err = multierror.Append(err, markErr)
			}
		} else {
			err = jobHandle.MarkComplete()
		}
	}()

	name, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(name); err != nil {
			// TODO - warn
		}
	}()

	filename, err := w.bundleManagerClient.GetUpload(context.Background(), upload.ID, name)
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

	if err := w.bundleManagerClient.SendDB(context.Background(), upload.ID, f); err != nil {
		return err
	}

	// TODO - use same txn
	if err := w.db.DeleteOverlappingDumps(context.Background(), nil, upload.RepositoryID, upload.Commit, upload.Root, upload.Indexer); err != nil {
		return err
	}

	tipCommit, err := gitserver.Head(w.db, upload.RepositoryID)
	if err != nil {
		return err
	}

	newCommits, err := gitserver.CommitsNear(w.db, upload.RepositoryID, tipCommit)
	if err != nil {
		return err
	}

	if tipCommit != upload.Commit {
		// If the tip is ahead of this commit, we also want to discover all of
		// the commits between this commit and the tip so that we can accurately
		// determine what is visible from the tip. If we do not do this before the
		// updateDumpsVisibleFromTip call below, no dumps will be reachable from
		// the tip and all dumps will be invisible.
		additionalCommits, err := gitserver.CommitsNear(w.db, upload.RepositoryID, upload.Commit)
		if err != nil {
			return err
		}

		for k, vs := range additionalCommits {
			newCommits[k] = append(newCommits[k], vs...)
		}
	}

	// TODO - use same txn
	// TODO - need to do same discover on query
	// TODO - determine if we know about these commits first
	if err := w.db.UpdateCommits(context.Background(), upload.RepositoryID, newCommits); err != nil {
		return err
	}

	// TODO - use same txn (is ok?)
	// TODO - there is a state mismatch here (we're still processing)
	if err := w.db.UpdateDumpsVisibleFromTip(context.Background(), nil, upload.RepositoryID, tipCommit); err != nil {
		return err
	}

	return nil
}
