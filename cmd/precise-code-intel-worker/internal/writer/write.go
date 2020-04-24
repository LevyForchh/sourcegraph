package writer

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/correlation"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/writer/schema"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/bundles/database"
	"github.com/sourcegraph/sourcegraph/internal/codeintel/bundles/types"
	"github.com/sourcegraph/sourcegraph/internal/sqliteutil"
)

const InternalVersion = "0.1.0"
const MaxNumResultChunks = 1000
const ResultsPerResultChunk = 500

type sqliteWriter struct {
	cx              *correlation.CorrelationState
	numResultChunks int
}

func Write(cx *correlation.CorrelationState, filename string) error {
	db, err := sqlx.Open("sqlite3_with_pcre", filename)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	if _, err := db.Exec(schema.TableDefinitions); err != nil {
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

		return (&sqliteWriter{cx: cx}).write(context.Background(), txn)
	}()

	if err != nil {
		return err
	}

	if _, err := db.Exec(schema.IndexDefinitions); err != nil {
		return err
	}

	return nil
}

func (w *sqliteWriter) write(ctx context.Context, tx *sql.Tx) (err error) {
	// Calculate the number of result chunks that we'll attempt to populate
	numResults := len(w.cx.DefinitionData) + len(w.cx.ReferenceData)
	w.numResultChunks = int(math.Min(MaxNumResultChunks, math.Max(1, math.Floor(float64(numResults)/ResultsPerResultChunk))))

	metaColumns := []string{"lsifVersion", "sourcegraphVersion", "numResultChunks"}
	documentsColumns := []string{"path", "data"}
	resultChunksColumns := []string{"id", "data"}
	definitionsReferencesColumns := []string{"scheme", "identifier", "documentPath", "startLine", "startCharacter", "endLine", "endCharacter"}

	metadataTableInserter := sqliteutil.NewBatchInserter(tx, "meta", metaColumns...)
	documentsTableInserter := sqliteutil.NewBatchInserter(tx, "documents", documentsColumns...)
	resultChunksTableInserter := sqliteutil.NewBatchInserter(tx, "resultChunks", resultChunksColumns...)
	definitionsTableInserter := sqliteutil.NewBatchInserter(tx, "definitions", definitionsReferencesColumns...)
	referencesTableInserter := sqliteutil.NewBatchInserter(tx, `references`, definitionsReferencesColumns...)

	fns := []func() error{
		func() error { return w.populateMetadataTable(ctx, metadataTableInserter) },
		func() error { return w.populateDocumentsTable(ctx, documentsTableInserter) },
		func() error { return w.populateResultChunksTable(ctx, resultChunksTableInserter) },
		func() error { return w.populateDefinitionsTable(ctx, definitionsTableInserter) },
		func() error { return w.populateReferencesTable(ctx, referencesTableInserter) },
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
}

func (w *sqliteWriter) populateMetadataTable(ctx context.Context, inserter *sqliteutil.BatchInserter) error {
	return inserter.Insert(ctx, w.cx.LsifVersion, InternalVersion, w.numResultChunks)
}

type DocumentDatas struct {
	Ranges             map[string]correlation.RangeData              `json:"ranges"`
	HoverResults       map[string]string                             `json:"hoverResults"`
	Monikers           map[string]correlation.MonikerData            `json:"monikers"`
	PackageInformation map[string]correlation.PackageInformationData `json:"packageInformation"`
}

func (w *sqliteWriter) populateDocumentsTable(ctx context.Context, inserter *sqliteutil.BatchInserter) error {
	// Gather and insert document data that includes the ranges contained in the document,
	// any associated hover data, and any associated moniker data/package information.
	// Each range also has identifiers that correlate to a definition or reference result
	// which can be found in a result chunk, created in the next step.

	for _, doc := range w.cx.DocumentData {
		if strings.HasPrefix(doc.URI, "..") {
			continue
		}

		document := DocumentDatas{
			Ranges:             map[string]correlation.RangeData{},
			HoverResults:       map[string]string{},
			Monikers:           map[string]correlation.MonikerData{},
			PackageInformation: map[string]correlation.PackageInformationData{},
		}

		for rangeID := range doc.Contains {
			r := w.cx.RangeData[rangeID]
			document.Ranges[rangeID] = r

			if r.HoverResultID != "" {
				hoverData := w.cx.HoverData[r.HoverResultID]
				document.HoverResults[r.HoverResultID] = hoverData
			}

			for monikerID := range r.MonikerIDs {
				moniker := w.cx.MonikerData[monikerID]
				document.Monikers[monikerID] = moniker

				if moniker.PackageInformationID != "" {
					packageInformation := w.cx.PackageInformationData[moniker.PackageInformationID]
					document.PackageInformation[moniker.PackageInformationID] = packageInformation
				}
			}
		}

		wrappedRanges := map[string][]json.RawMessage{"value": nil}
		wrappedHoverResults := map[string][]json.RawMessage{"value": nil}
		wrappedMonikers := map[string][]json.RawMessage{"value": nil}
		wrappedPackageInformation := map[string][]json.RawMessage{"value": nil}

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
				"monikerIds":         map[string]interface{}{"value": r},
			}})
			if err != nil {
				return err
			}

			wrappedRanges["value"] = append(wrappedRanges["value"], ser)
		}

		for k, v := range document.HoverResults {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedHoverResults["value"] = append(wrappedHoverResults["value"], ser)
		}

		for k, v := range document.Monikers {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedMonikers["value"] = append(wrappedMonikers["value"], ser)
		}

		for k, v := range document.PackageInformation {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedPackageInformation["value"] = append(wrappedPackageInformation["value"], ser)
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

func (w *sqliteWriter) populateResultChunksTable(ctx context.Context, inserter *sqliteutil.BatchInserter) error {
	var resultChunks []ResultChunk
	for i := 0; i < w.numResultChunks; i++ {
		resultChunks = append(resultChunks, ResultChunk{
			Paths:              map[string]string{},
			DocumentIDRangeIDs: map[string][]DocumentIDRangeID{},
		})
	}

	addToChunk(w.cx, resultChunks, w.cx.DefinitionData)
	addToChunk(w.cx, resultChunks, w.cx.ReferenceData)

	for id, resultChunk := range resultChunks {
		if len(resultChunk.Paths) == 0 && len(resultChunk.DocumentIDRangeIDs) == 0 {
			continue
		}

		wrappedDocumentPaths := map[string][]json.RawMessage{"value": nil}
		wrappedDocumentIdRangeIds := map[string][]json.RawMessage{"value": nil}

		for k, v := range resultChunk.Paths {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedDocumentPaths["value"] = append(wrappedDocumentPaths["value"], ser)
		}

		for k, v := range resultChunk.DocumentIDRangeIDs {
			ser, err := json.Marshal([]interface{}{k, v})
			if err != nil {
				return err
			}

			wrappedDocumentIdRangeIds["value"] = append(wrappedDocumentIdRangeIds["value"], ser)
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

func addToChunk(cx *correlation.CorrelationState, resultChunks []ResultChunk, data map[string]correlation.DefaultIDSetMap) {
	for id, documentRanges := range data {
		resultChunk := resultChunks[database.HashKey(types.ID(id), len(resultChunks))]

		for documentID, rangeIDs := range documentRanges {
			doc := cx.DocumentData[documentID]
			resultChunk.Paths[documentID] = doc.URI

			for rangeID := range rangeIDs {
				resultChunk.DocumentIDRangeIDs[id] = append(resultChunk.DocumentIDRangeIDs[id], DocumentIDRangeID{documentID, rangeID})
			}
		}
	}
}

func (w *sqliteWriter) populateDefinitionsTable(ctx context.Context, inserter *sqliteutil.BatchInserter) error {
	definitionMonikers := correlation.DefaultIDSetMap{}
	for _, r := range w.cx.RangeData {
		if r.DefinitionResultID != "" && len(r.MonikerIDs) > 0 {
			s := definitionMonikers.GetOrCreate(r.DefinitionResultID)
			for id := range r.MonikerIDs {
				s.Add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, w.cx, w.cx.DefinitionData, definitionMonikers, inserter)
}

func (w *sqliteWriter) populateReferencesTable(ctx context.Context, inserter *sqliteutil.BatchInserter) error {
	referenceMonikers := correlation.DefaultIDSetMap{}
	for _, r := range w.cx.RangeData {
		if r.ReferenceResultID != "" && len(r.MonikerIDs) > 0 {
			s := referenceMonikers.GetOrCreate(r.ReferenceResultID)
			for id := range r.MonikerIDs {
				s.Add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, w.cx, w.cx.ReferenceData, referenceMonikers, inserter)
}

func insertMonikerRanges(ctx context.Context, cx *correlation.CorrelationState, data map[string]correlation.DefaultIDSetMap, monikers correlation.DefaultIDSetMap, inserter *sqliteutil.BatchInserter) error {
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
