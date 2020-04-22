package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/worker"
	"github.com/sourcegraph/sourcegraph/internal/conf"
	"github.com/sourcegraph/sourcegraph/internal/db/dbconn"
	"github.com/sourcegraph/sourcegraph/internal/debugserver"
	"github.com/sourcegraph/sourcegraph/internal/env"
	"github.com/sourcegraph/sourcegraph/internal/sqliteutil"
	"github.com/sourcegraph/sourcegraph/internal/tracer"
)

func main() {
	env.Lock()
	env.HandleHelpFlag()
	tracer.Init()

	sqliteutil.MustRegisterSqlite3WithPcre()

	var (
		pollInterval     = mustParseInterval(rawPollInterval, "POLL_INTERVAL")
		bundleManagerURL = mustGet(rawBundleManagerURL, "BUNDLE_MANAGER_URL")
	)

	db := mustInitializeDatabase()

	workerImpl := worker.New(worker.WorkerOpts{
		DB:                  db,
		BundleManagerClient: bundles.New(bundleManagerURL),
		PollInterval:        pollInterval,
	})

	go func() { _ = workerImpl.Start() }()
	go debugserver.Start()
	waitForSignal()
}

func mustInitializeDatabase() db.DB {
	postgresDSN := conf.Get().ServiceConnections.PostgresDSN
	conf.Watch(func() {
		if newDSN := conf.Get().ServiceConnections.PostgresDSN; postgresDSN != newDSN {
			log.Fatalf("Detected repository DSN change, restarting to take effect: %s", newDSN)
		}
	})

	db, err := db.New(postgresDSN)
	if err != nil {
		log.Fatalf("failed to initialize db store: %s", err)
	}

	// TODO - sick, get rid of the calls into the frontend db package
	dbconn.ConnectToDB(postgresDSN)
	return db
}

func waitForSignal() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGHUP)

	for i := 0; i < 2; i++ {
		<-signals
	}

	os.Exit(0)
}
