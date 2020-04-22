package worker

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/converter"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
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

		upload, closer, ok, err := w.db.Dequeue(context.Background())
		if err != nil {
			return err
		}
		if !ok {
			time.Sleep(w.pollInterval)
			continue
		}

		fmt.Printf("Processing\n")
		if err := w.process(upload, closer); err != nil {
			fmt.Printf("FAILED TO PROCESS: %#v\n", err)
			return err
		}

		fmt.Printf("PROCESSED!\n")
	}
}

func (w *Worker) process(upload db.Upload, closer db.TxCloser) (err error) {
	defer func() {
		// TODO - mark complete or error
		err = closer.CloseTx(err)
	}()

	name, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer func() {
		// TODO - catch error
		_ = os.RemoveAll(name)
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

	// packages, refs
	_, _, err2 := converter.Convert(w.db, upload.RepositoryID, upload.Commit, upload.Root, filename, newFilename)
	if err2 != nil {
		return err2
	}

	// TODO - TW
	// TODO - unify types here
	// if err := w.db.UpdatePackagesAndRefs(context.Background(), nil, upload.ID, packages, refs); err != nil {
	// 	return err
	// }

	f, err := os.Open(newFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := w.bundleManagerClient.SendDB(context.Background(), upload.ID, f); err != nil {
		return err
	}

	// TODO - delete overwritten dumps
	// TODO

	// TODO - update commits and dumps visible from tip
	// TODO

	return nil
}
