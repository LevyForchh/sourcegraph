package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	bundles "github.com/sourcegraph/sourcegraph/internal/codeintel/bundles/client"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/db"
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

	err = process(context.Background(), w.db, w.bundleManagerClient, upload, jobHandle)
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
