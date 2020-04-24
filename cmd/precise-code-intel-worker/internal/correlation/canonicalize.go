package correlation

import (
	"sort"
)

func canonicalize(cx *CorrelationState) {
	fns := []func(cx *CorrelationState){
		// Determine if multiple documents are defined with the same URI. This happens in
		// some indexers (such as lsif-tsc) that index dependent projects into the same
		// dump as the target project. For each set of documents that share a path, we
		// choose one document to be the canonical representative and merge the contains,
		// definition, and reference data into the unique canonical document.
		canonicalizeDocuments,
		// Determine which reference results are linked together. Determine a canonical
		// reference result for each set so that we can remap all identifiers to the
		// chosen one.
		canonicalizeReferenceResults,
		// // Collapse result sets data into the ranges that can reach them. The
		// // remainder of this function assumes that we can completely ignore
		// // the "next" edges coming from range data.
		canonicalizeResultSets,
		canonicalizeRanges,
	}

	for _, fn := range fns {
		fn(cx)
	}
}

// * Merge the data in the correlator of all documents that share the same path. This
// * function works by moving the contains, definition, and reference data keyed by a
// * document with a duplicate path into a canonical document with that path. The first
// * document inserted for a path is the canonical document for that path. This function
// * guarantees that duplicate document ids are removed from these maps.
func canonicalizeDocuments(cx *CorrelationState) {
	qr := map[string][]string{}
	for documentID, doc := range cx.DocumentData {
		qr[doc.URI] = append(qr[doc.URI], documentID)
	}
	for _, v := range qr {
		sort.Strings(v)
	}

	for documentID, doc := range cx.DocumentData {
		canonicalDocumentID := qr[doc.URI][0]
		if documentID == canonicalDocumentID {
			continue
		}

		for id := range cx.DocumentData[documentID].Contains {
			cx.DocumentData[canonicalDocumentID].Contains.Add(id)
		}
		delete(cx.DocumentData, documentID)

		for _, rangeIDsByDocumentID := range cx.DefinitionData {
			if rangeIDs, ok := rangeIDsByDocumentID[documentID]; ok {
				rangeIDsByDocumentID.GetOrCreate(canonicalDocumentID).AddAll(rangeIDs)
				delete(rangeIDsByDocumentID, documentID)
			}
		}

		for _, rangeIDsByDocumentID := range cx.ReferenceData {
			if rangeIDs, ok := rangeIDsByDocumentID[documentID]; ok {
				rangeIDsByDocumentID.GetOrCreate(canonicalDocumentID).AddAll(rangeIDs)
				delete(rangeIDsByDocumentID, documentID)
			}
		}
	}
}

// * Determine which reference result sets are linked via item edges. Choose a canonical
// * reference result from each batch. Merge all data into the canonical result and remove
// * all non-canonical results from the correlator (note: this leave unlinked results alone).
// * Return a map from reference result identifier to the identifier of the canonical result.
func canonicalizeReferenceResults(cx *CorrelationState) {
	referenceResultIDToCanonicalReferenceResultIDs := map[string]string{}
	for referenceResultID := range cx.LinkedReferenceResults {
		// Don't re-process the same set of linked reference results
		if _, ok := referenceResultIDToCanonicalReferenceResultIDs[referenceResultID]; ok {
			continue
		}

		// Find all reachable items and order them deterministically
		linkedIDs := cx.LinkedReferenceResults.ExtractSet(referenceResultID)
		canonicalID, _ := linkedIDs.Choose()
		canonicalReferenceResult := cx.ReferenceData[canonicalID]

		for linkedID := range linkedIDs {
			// Link each id to its canonical representation. We do this for
			// the `linkedId === canonicalId` case so we can reliably detect
			// duplication at the start of this loop.

			referenceResultIDToCanonicalReferenceResultIDs[linkedID] = canonicalID
			if linkedID == canonicalID {
				continue
			}

			// If it's a different identifier, then normalize all data from the linked result
			// set into the canonical one.
			for documentID, rangeIDs := range cx.ReferenceData[linkedID] {
				canonicalReferenceResult.GetOrCreate(documentID).AddAll(rangeIDs)
			}
		}
	}

	for id, item := range cx.RangeData {
		if canonicalID, ok := referenceResultIDToCanonicalReferenceResultIDs[item.ReferenceResultID]; ok {
			cx.RangeData[id] = item.setReferenceResultID(canonicalID)
		}
	}

	for id, item := range cx.ResultSetData {
		if canonicalID, ok := referenceResultIDToCanonicalReferenceResultIDs[item.ReferenceResultID]; ok {
			cx.ResultSetData[id] = item.setReferenceResultID(canonicalID)
		}
	}

	canonicalReferenceResultIDs := map[string]struct{}{}
	for _, canonicalID := range referenceResultIDToCanonicalReferenceResultIDs {
		canonicalReferenceResultIDs[canonicalID] = struct{}{}
	}

	for referenceResultID := range referenceResultIDToCanonicalReferenceResultIDs {
		if _, ok := canonicalReferenceResultIDs[referenceResultID]; !ok {
			delete(cx.ReferenceData, referenceResultID)
		}
	}
}

func canonicalizeResultSets(cx *CorrelationState) {
	for resultSetID, resultSetData := range cx.ResultSetData {
		canonicalizeResultSetData(cx, resultSetID, resultSetData)
	}

	for resultSetID, resultSetData := range cx.ResultSetData {
		cx.ResultSetData[resultSetID] = resultSetData.setMonikerIDs(gatherMonikers(cx, resultSetData.MonikerIDs))
	}
}

func canonicalizeRanges(cx *CorrelationState) {
	for rangeID, rangeData := range cx.RangeData {
		if _, nextItem, ok := next(cx, rangeID); ok {
			rangeData = mergeNextRangeData(rangeData, nextItem)
			delete(cx.NextData, rangeID)
		}

		rd := rangeData.setMonikerIDs(gatherMonikers(cx, rangeData.MonikerIDs))
		cx.RangeData[rangeID] = rd
	}
}

func canonicalizeResultSetData(cx *CorrelationState, id string, item ResultSetData) ResultSetData {
	if nextID, nextItem, ok := next(cx, id); ok {
		item = mergeNextResultSetData(item, canonicalizeResultSetData(cx, nextID, nextItem))
		cx.ResultSetData[id] = item
		delete(cx.NextData, id)
	}

	return item
}

func mergeNextResultSetData(item, nextItem ResultSetData) ResultSetData {
	if item.DefinitionResultID == "" {
		item = item.setDefinitionResultID(nextItem.DefinitionResultID)
	}
	if item.ReferenceResultID == "" {
		item = item.setReferenceResultID(nextItem.ReferenceResultID)
	}
	if item.HoverResultID == "" {
		item = item.setHoverResultID(nextItem.HoverResultID)
	}

	item.MonikerIDs.AddAll(nextItem.MonikerIDs)
	return item
}

func mergeNextRangeData(item RangeData, nextItem ResultSetData) RangeData {
	if item.DefinitionResultID == "" {
		item = item.setDefinitionResultID(nextItem.DefinitionResultID)
	}
	if item.ReferenceResultID == "" {
		item = item.setReferenceResultID(nextItem.ReferenceResultID)
	}
	if item.HoverResultID == "" {
		item = item.setHoverResultID(nextItem.HoverResultID)
	}

	item.MonikerIDs.AddAll(nextItem.MonikerIDs)
	return item
}

func gatherMonikers(cx *CorrelationState, source IDSet) IDSet {
	monikers := IDSet{}
	for sourceID := range source {
		for id := range cx.LinkedMonikers.ExtractSet(sourceID) {
			if cx.MonikerData[id].Kind != "local" {
				monikers.Add(id)
			}
		}
	}

	return monikers
}

func next(cx *CorrelationState, id string) (string, ResultSetData, bool) {
	nextID, ok := cx.NextData[id]
	if !ok {
		return "", ResultSetData{}, false
	}

	return nextID, cx.ResultSetData[nextID], true
}
