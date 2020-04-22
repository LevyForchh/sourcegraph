package converter

import (
	"context"
	"math"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/sqliteutil"
)

func Write(cx *CorrelationState, filename string) (err error) {
	ctx := context.Background()

	db, err := sqlx.Open("sqlite3_with_pcre", filename)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := db.Close()
		if err != nil {
			// TODO - wrap error
		} else {
			err = closeErr
		}
	}()

	txn, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() {
		// TODO - uhh, something like this I guess
		if err != nil {
			err = txn.Commit()
		} else {
			err = txn.Rollback()
		}
	}()

	pragmaStmts := []string{
		PragmaA,
		PragmaB,
	}

	for _, stmt := range pragmaStmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	stmts := []string{
		CreateTableDefinitions,
		CreateTableDocuments,
		CreateTableMeta,
		CreateTableReferences,
		CreateTableResultChunks,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	// Calculate the number of result chunks that we'll attempt to populate
	numResults := len(cx.definitionData) + len(cx.referenceData)
	numResultChunks := int(math.Min(MaxNumResultChunks, math.Max(1, math.Floor(float64(numResults)/ResultsPerResultChunk))))

	// TODO - insert inside a txn
	metadataTableInserter := sqliteutil.NewBatchInserter(txn, "meta", "lsifVersion", "sourcegraphVersion", "numResultChunks")
	documentsTableInserter := sqliteutil.NewBatchInserter(txn, "documents", "path", "data")
	resultChunksTableInserter := sqliteutil.NewBatchInserter(txn, "resultChunks", "id", "data")
	definitionsTableInserter := sqliteutil.NewBatchInserter(txn, "definitions", "scheme", "identifier", "documentPath", "startLine", "endLine", "startCharacter", "endCharacter")
	referencesTableInserter := sqliteutil.NewBatchInserter(txn, `"references"`, "scheme", "identifier", "documentPath", "startLine", "endLine", "startCharacter", "endCharacter")

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

	indexStmts := []string{
		CreateDefinitionsIndex,
		CreateReferencesIndex,
	}

	for _, stmt := range indexStmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func populateMetadataTable(ctx context.Context, cx *CorrelationState, numResultChunks int, inserter *sqliteutil.BatchInserter) error {
	return inserter.Insert(ctx, cx.lsifVersion, InternalVersion, numResultChunks)
}

// TODO - need to serialize all stupid
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

	for _, doc := range cx.documentData {
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
			r := cx.rangeData[rangeID]
			document.Ranges[rangeID] = r

			if r.HoverResultID != "" {
				hoverData := cx.hoverData[r.HoverResultID]
				document.HoverResults[r.HoverResultID] = hoverData
			}

			for monikerID := range r.MonikerIDs {
				moniker := cx.monikerData[monikerID]
				document.Monikers[monikerID] = moniker

				if moniker.PackageInformationID != "" {
					packageInformation := cx.packageInformationData[moniker.PackageInformationID]
					document.PackageInformation[moniker.PackageInformationID] = packageInformation
				}
			}
		}

		// Create document record from the correlated information. This will also insert
		// external definitions and references into the maps initialized above, which are
		// inserted into the definitions and references table, respectively, below.
		data, err := gzipJSON(document)
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

	addToChunk(cx, resultChunks, cx.definitionData)
	addToChunk(cx, resultChunks, cx.referenceData)

	for id, resultChunk := range resultChunks {
		if len(resultChunk.Paths) == 0 && len(resultChunk.DocumentIDRangeIDs) == 0 {
			continue
		}

		data, err := gzipJSON(resultChunk)
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
			doc, ok := cx.documentData[documentID]
			if !ok {
				panic("Should not happen")
			}

			for rangeID := range rangeIDs {
				resultChunk.DocumentIDRangeIDs[id] = append(resultChunk.DocumentIDRangeIDs[id], DocumentIDRangeID{documentID, rangeID})
			}
			resultChunk.Paths[documentID] = doc.URI
		}
	}
}

func populateDefinitionsTable(ctx context.Context, cx *CorrelationState, inserter *sqliteutil.BatchInserter) error {
	// Determine the set of monikers that are attached to a definition or a
	// reference result. Correlating information in this way has two benefits:
	//   (1) it reduces duplicates in the definitions and references tables
	//   (2) it stop us from re-iterating over the range data of the entire
	//       LSIF dump, which is by far the largest proportion of data.

	definitionMonikers := defaultIDSetMap{}
	for _, r := range cx.rangeData {
		if r.DefinitionResultID != "" && len(r.MonikerIDs) > 0 {
			s := definitionMonikers.getOrCreate(r.DefinitionResultID)
			for id := range r.MonikerIDs {
				s.add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, cx, cx.definitionData, definitionMonikers, inserter)
}

func populateReferencesTable(ctx context.Context, cx *CorrelationState, inserter *sqliteutil.BatchInserter) error {
	// Determine the set of monikers that are attached to a definition or a
	// reference result. Correlating information in this way has two benefits:
	//   (1) it reduces duplicates in the definitions and references tables
	//   (2) it stop us from re-iterating over the range data of the entire
	//       LSIF dump, which is by far the largest proportion of data.

	referenceMonikers := defaultIDSetMap{}
	for _, r := range cx.rangeData {
		if r.ReferenceResultID != "" && len(r.MonikerIDs) > 0 {
			s := referenceMonikers.getOrCreate(r.ReferenceResultID)
			for id := range r.MonikerIDs {
				s.add(id)
			}
		}
	}

	return insertMonikerRanges(ctx, cx, cx.referenceData, referenceMonikers, inserter)
}

func insertMonikerRanges(ctx context.Context, cx *CorrelationState, data map[string]defaultIDSetMap, monikers defaultIDSetMap, inserter *sqliteutil.BatchInserter) error {
	for id, documentRanges := range data {
		// Get monikers. Nothing to insert if we don't have any.
		monikerIDs, ok := monikers[id]
		if !ok {
			continue
		}

		// Correlate each moniker with the document/range pairs stored in
		// the result set provided by the data argument of this function.

		for monikerID := range monikerIDs {
			moniker := cx.monikerData[monikerID]

			for documentID, rangeIDs := range documentRanges {

				doc, ok := cx.documentData[documentID]
				if !ok {
					panic("Should not happen")
				}
				if strings.HasPrefix(doc.URI, "..") {
					// Skip definitions or references that point to a document that are not
					// present in the dump. Including this would cause a query that always
					// fails when it cannot resolve the missing document data.
					continue
				}

				for id := range rangeIDs {
					r := cx.rangeData[id]

					if err := inserter.Insert(ctx, moniker.Scheme, moniker.Identifier, doc.URI, r.StartLine, r.StartCharacter, r.EndLine, r.EndCharacter); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}
