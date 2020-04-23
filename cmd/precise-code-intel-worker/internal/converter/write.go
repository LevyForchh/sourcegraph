package converter

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/sqliteutil"
)

const MaxNumResultChunks = 1000
const ResultsPerResultChunk = 500
const InternalVersion = "0.1.0"

// TODO - put in bindata?
var schema = `
	CREATE TABLE "meta" (
		"id" integer PRIMARY KEY NOT NULL,
		"lsifVersion" text NOT NULL,
		"sourcegraphVersion" text NOT NULL,
		"numResultChunks" integer NOT NULL
	);

	CREATE TABLE "documents" (
		"path" text PRIMARY KEY NOT NULL,
		"data" blob NOT NULL
	);

	CREATE TABLE "resultChunks" (
		"id" integer PRIMARY KEY NOT NULL,
		"data" blob NOT NULL
	);

	CREATE TABLE "definitions" (
		"id" integer PRIMARY KEY NOT NULL,
		"scheme" text NOT NULL,
		"identifier" text NOT NULL,
		"documentPath" text NOT NULL,
		"startLine" integer NOT NULL,
		"endLine" integer NOT NULL,
		"startCharacter" integer NOT NULL,
		"endCharacter" integer NOT NULL
	);

	CREATE TABLE "references" (
		"id" integer PRIMARY KEY NOT NULL,
		"scheme" text NOT NULL,
		"identifier" text NOT NULL,
		"documentPath" text NOT NULL,
		"startLine" integer NOT NULL,
		"endLine" integer NOT NULL,
		"startCharacter" integer NOT NULL,
		"endCharacter" integer NOT NULL
	);

	PRAGMA synchronous = OFF;
	PRAGMA journal_mode = OFF;
`

var indexes = `
	CREATE INDEX "idx_definitions" ON "definitions" ("scheme", "identifier");
	CREATE INDEX "idx_references" ON "references" ("scheme", "identifier");
`

func sqlite(filename string, fn func(tx *sql.Tx) error) error {
	db, err := sqlx.Open("sqlite3_with_pcre", filename)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	if _, err := db.Exec(schema); err != nil {
		return err
	}

	err = func() error {
		txn, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				if rollErr := txn.Rollback(); rollErr != nil {
					err = multierror.Append(err, rollErr)
				}
			} else {
				err = txn.Commit()
			}
		}()

		return fn(txn)
	}()

	if err != nil {
		return err
	}

	if _, err := db.Exec(indexes); err != nil {
		return err
	}

	return nil
}

func Write(cx *CorrelationState, filename string) (err error) {
	ctx := context.Background()
	// Calculate the number of result chunks that we'll attempt to populate
	numResults := len(cx.DefinitionData) + len(cx.ReferenceData)
	numResultChunks := int(math.Min(MaxNumResultChunks, math.Max(1, math.Floor(float64(numResults)/ResultsPerResultChunk))))

	return sqlite(filename, func(txn *sql.Tx) error {
		metadataTableInserter := sqliteutil.NewBatchInserter(txn, "meta", "lsifVersion", "sourcegraphVersion", "numResultChunks")
		documentsTableInserter := sqliteutil.NewBatchInserter(txn, "documents", "path", "data")
		resultChunksTableInserter := sqliteutil.NewBatchInserter(txn, "resultChunks", "id", "data")
		definitionsTableInserter := sqliteutil.NewBatchInserter(txn, "definitions", "scheme", "identifier", "documentPath", "startLine", "startCharacter", "endLine", "endCharacter")
		referencesTableInserter := sqliteutil.NewBatchInserter(txn, `references`, "scheme", "identifier", "documentPath", "startLine", "startCharacter", "endLine", "endCharacter")

		fns := []func() error{
			func() error { return populateMetadataTable(ctx, cx, numResultChunks, metadataTableInserter) },
			func() error { return populateDocumentsTable(ctx, cx, documentsTableInserter) },
			func() error { return populateResultChunksTable(ctx, cx, numResultChunks, resultChunksTableInserter) },
			func() error { return populateDefinitionsTable(ctx, cx, definitionsTableInserter) },
			func() error { return populateReferencesTable(ctx, cx, referencesTableInserter) },
		}

		for _, fn := range fns {
			if err := fn(); err != nil {
				return err
			}
		}

		inserters := []*sqliteutil.BatchInserter{
			metadataTableInserter,
			documentsTableInserter,
			resultChunksTableInserter,
			definitionsTableInserter,
			referencesTableInserter,
		}

		for _, inserter := range inserters {
			if err := inserter.Flush(ctx); err != nil {
				return err
			}
		}

		return nil
	})
}

func populateMetadataTable(ctx context.Context, cx *CorrelationState, numResultChunks int, inserter *sqliteutil.BatchInserter) error {
	return inserter.Insert(ctx, cx.LsifVersion, InternalVersion, numResultChunks)
}

type DocumentDatas struct {
	Ranges             map[string]RangeData              `json:"ranges"`
	HoverResults       map[string]string                 `json:"hoverResults"`
	Monikers           map[string]MonikerData            `json:"monikers"`
	PackageInformation map[string]PackageInformationData `json:"packageInformation"`
}

func populateDocumentsTable(ctx context.Context, cx *CorrelationState, inserter *sqliteutil.BatchInserter) error {
	// Gather and insert document data that includes the ranges contained in the document,
	// any associated hover data, and any associated moniker data/package information.
	// Each range also has identifiers that correlate to a definition or reference result
	// which can be found in a result chunk, created in the next step.

	for _, doc := range cx.DocumentData {
		if strings.HasPrefix(doc.URI, "..") {
			continue
		}

		document := DocumentDatas{
			Ranges:             map[string]RangeData{},
			HoverResults:       map[string]string{},
			Monikers:           map[string]MonikerData{},
			PackageInformation: map[string]PackageInformationData{},
		}

		for rangeID := range doc.Contains {
			r := cx.RangeData[rangeID]
			document.Ranges[rangeID] = r

			if r.HoverResultID != "" {
				hoverData := cx.HoverData[r.HoverResultID]
				document.HoverResults[r.HoverResultID] = hoverData
			}

			for monikerID := range r.MonikerIDs {
				moniker := cx.MonikerData[monikerID]
				document.Monikers[monikerID] = moniker

				if moniker.PackageInformationID != "" {
					packageInformation := cx.PackageInformationData[moniker.PackageInformationID]
					document.PackageInformation[moniker.PackageInformationID] = packageInformation
				}
			}
		}

		var wrappedRanges wrappedMapValue
		var wrappedHoverResults wrappedMapValue
		var wrappedMonikers wrappedMapValue
		var wrappedPackageInformation wrappedMapValue

		for k, v := range document.Ranges {
			var r []json.RawMessage
			for id := range v.MonikerIDs {
				serx, err := json.Marshal(id)
				if err != nil {
					return err
				}

				r = append(r, serx)
			}

			ser, err := json.Marshal([]interface{}{k, map[string]interface{}{
				"startLine":          v.StartLine,
				"startCharacter":     v.StartCharacter,
				"endLine":            v.EndLine,
				"endCharacter":       v.EndCharacter,
				"definitionResultId": v.DefinitionResultID,
				"referenceResultId":  v.ReferenceResultID,
				"hoverResultId":      v.HoverResultID,
				"monikerIds":         wrappedSetValue{Value: r},
			}})
			if err != nil {
				return err
			}

			wrappedRanges.Value = append(wrappedRanges.Value, ser)
		}

		for k, v := range document.HoverResults {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedHoverResults.Value = append(wrappedHoverResults.Value, ser)
		}

		for k, v := range document.Monikers {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedMonikers.Value = append(wrappedMonikers.Value, ser)
		}

		for k, v := range document.PackageInformation {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedPackageInformation.Value = append(wrappedPackageInformation.Value, ser)
		}

		// Create document record from the correlated information. This will also insert
		// external definitions and references into the maps initialized above, which are
		// inserted into the definitions and references table, respectively, below.
		data, err := gzipJSON(map[string]interface{}{
			"ranges":             wrappedRanges,
			"hoverResults":       wrappedHoverResults,
			"monikers":           wrappedMonikers,
			"packageInformation": wrappedPackageInformation,
		})
		if err != nil {
			return err
		}

		if err := inserter.Insert(ctx, doc.URI, data); err != nil {
			return err
		}
	}

	return nil
}

type ResultChunk struct {
	Paths              map[string]string              `json:"paths"`
	DocumentIDRangeIDs map[string][]DocumentIDRangeID `json:"documentIdRangeIds"`
}

type DocumentIDRangeID struct {
	DocumentID string `json:"documentId"`
	RangeID    string `json:"rangeId"`
}

func populateResultChunksTable(ctx context.Context, cx *CorrelationState, numResultChunks int, inserter *sqliteutil.BatchInserter) error {
	var resultChunks []ResultChunk
	for i := 0; i < numResultChunks; i++ {
		resultChunks = append(resultChunks, ResultChunk{
			Paths:              map[string]string{},
			DocumentIDRangeIDs: map[string][]DocumentIDRangeID{},
		})
	}

	addToChunk(cx, resultChunks, cx.DefinitionData)
	addToChunk(cx, resultChunks, cx.ReferenceData)

	for id, resultChunk := range resultChunks {
		if len(resultChunk.Paths) == 0 && len(resultChunk.DocumentIDRangeIDs) == 0 {
			continue
		}

		var wrappedDocumentPaths wrappedMapValue
		var wrappedDocumentIdRangeIds wrappedMapValue

		for k, v := range resultChunk.Paths {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedDocumentPaths.Value = append(wrappedDocumentPaths.Value, ser)
		}

		for k, v := range resultChunk.DocumentIDRangeIDs {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedDocumentIdRangeIds.Value = append(wrappedDocumentIdRangeIds.Value, ser)
		}

		gx := map[string]interface{}{
			"documentPaths":      wrappedDocumentPaths,
			"documentIdRangeIds": wrappedDocumentIdRangeIds,
		}

		data, err := gzipJSON(gx)
		if err != nil {
			return err
		}

		if err := inserter.Insert(ctx, id, data); err != nil {
			return err
		}
	}

	return nil
}

func addToChunk(cx *CorrelationState, resultChunks []ResultChunk, data map[string]defaultIDSetMap) {
	for id, documentRanges := range data {
		resultChunk := resultChunks[hashKey(id, len(resultChunks))]

		for documentID, rangeIDs := range documentRanges {
			doc := cx.DocumentData[documentID]
			resultChunk.Paths[documentID] = doc.URI

			for rangeID := range rangeIDs {
				resultChunk.DocumentIDRangeIDs[id] = append(resultChunk.DocumentIDRangeIDs[id], DocumentIDRangeID{documentID, rangeID})
			}
		}
	}
}

func populateDefinitionsTable(ctx context.Context, cx *CorrelationState, inserter *sqliteutil.BatchInserter) error {
	definitionMonikers := defaultIDSetMap{}
	for _, r := range cx.RangeData {
		if r.DefinitionResultID != "" && len(r.MonikerIDs) > 0 {
			s := definitionMonikers.getOrCreate(r.DefinitionResultID)
			for id := range r.MonikerIDs {
				s.add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, cx, cx.DefinitionData, definitionMonikers, inserter)
}

func populateReferencesTable(ctx context.Context, cx *CorrelationState, inserter *sqliteutil.BatchInserter) error {
	referenceMonikers := defaultIDSetMap{}
	for _, r := range cx.RangeData {
		if r.ReferenceResultID != "" && len(r.MonikerIDs) > 0 {
			s := referenceMonikers.getOrCreate(r.ReferenceResultID)
			for id := range r.MonikerIDs {
				s.add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, cx, cx.ReferenceData, referenceMonikers, inserter)
}

func insertMonikerRanges(ctx context.Context, cx *CorrelationState, data map[string]defaultIDSetMap, monikers defaultIDSetMap, inserter *sqliteutil.BatchInserter) error {
	for id, documentRanges := range data {
		monikerIDs, ok := monikers[id]
		if !ok {
			continue
		}

		for monikerID := range monikerIDs {
			moniker := cx.MonikerData[monikerID]

			for documentID, rangeIDs := range documentRanges {
				doc := cx.DocumentData[documentID]

				if strings.HasPrefix(doc.URI, "..") {
					continue
				}

				for id := range rangeIDs {
					r := cx.RangeData[id]

					if err := inserter.Insert(ctx, moniker.Scheme, moniker.Identifier, doc.URI, r.StartLine, r.StartCharacter, r.EndLine, r.EndCharacter); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}
