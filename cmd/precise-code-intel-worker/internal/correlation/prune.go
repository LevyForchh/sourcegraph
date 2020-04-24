package correlation

import (
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/existence"
)

func Prune(fn existence.GetChildrenFunc, root string, cx *CorrelationState) error {
	var paths []string
	for _, doc := range cx.DocumentData {
		paths = append(paths, doc.URI)
	}

	ec, err := existence.NewExistenceChecker(root, paths, fn)
	if err != nil {
		return err
	}

	for documentID, doc := range cx.DocumentData {
		// Do not gather any document that is not within the dump root or does not exist
		// in git. If the path is outside of the dump root, then it will never be queried
		// as the current text document path and the dump root are compared to determine
		// which dump to open. If the path does not exist in git, it will also never be
		// queried.
		if !ec.ShouldInclude(doc.URI) {
			delete(cx.DocumentData, documentID)
		}
	}

	for _, documentRanges := range cx.DefinitionData {
		for documentID := range documentRanges {
			if _, ok := cx.DocumentData[documentID]; !ok {
				// Skip pointing to locations that are not available in git. This can occur
				// with indexers that point to generated files or dependencies that are not
				// committed (e.g. node_modules). Keeping these in the dump can cause the
				// UI to redirect to a path that doesn't exist.
				delete(documentRanges, documentID)
			}
		}
	}

	for _, documentRanges := range cx.ReferenceData {
		for documentID := range documentRanges {
			if _, ok := cx.DocumentData[documentID]; !ok {
				// Skip pointing to locations that are not available in git. This can occur
				// with indexers that point to generated files or dependencies that are not
				// committed (e.g. node_modules). Keeping these in the dump can cause the
				// UI to redirect to a path that doesn't exist.
				delete(documentRanges, documentID)
			}
		}
	}

	return nil
}
