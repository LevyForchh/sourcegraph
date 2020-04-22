package converter

import (
	"fmt"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
)

const MaxNumResultChunks = 1000
const ResultsPerResultChunk = 500
const InternalVersion = "0.1.0"

type Package struct {
	Scheme  string
	Name    string
	Version string
}

type Reference struct {
	Scheme      string
	Name        string
	Version     string
	Identifiers []string
}

func Convert(db db.DB, repositoryID int, commit, root, filename, newFilename string) (_ []Package, _ []Reference, err error) {
	cx, err := correlate(filename, root)
	if err != nil {
		return nil, nil, err
	}

	canonicalize(cx)

	if err := Prune(db, repositoryID, commit, root, cx); err != nil {
		return nil, nil, err
	}

	if err := Write(cx, newFilename); err != nil {
		return nil, nil, err
	}

	// TODO - de-duplicate
	var packages []Package
	for id := range cx.exportedMonikers {
		source := cx.monikerData[id]
		packageInfo := cx.packageInformationData[source.PackageInformationID]
		packages = append(packages, Package{
			Scheme:  source.Scheme,
			Name:    packageInfo.Name,
			Version: packageInfo.Version,
		})
	}

	// TODO - flatten
	references := map[string]Reference{}
	for id := range cx.importedMonikers {
		source := cx.monikerData[id]
		packageInfo := cx.packageInformationData[source.PackageInformationID]
		key := fmt.Sprintf("%s:%s:%s", source.Scheme, packageInfo.Name, packageInfo.Version)
		references[key] = Reference{
			Scheme:      source.Scheme,
			Name:        packageInfo.Name,
			Version:     packageInfo.Version,
			Identifiers: append(references[key].Identifiers, source.Identifier),
		}
	}

	var refs []Reference
	for _, ref := range references {
		refs = append(refs, ref)
	}

	return packages, refs, nil
}
