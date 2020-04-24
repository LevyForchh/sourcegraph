package worker

import (
	"fmt"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/correlation"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/existence"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/writer"
)

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

func convert(fn existence.GetChildrenFunc, root, filename, newFilename string) (_ []Package, _ []Reference, err error) {
	cx, err := correlation.Correlate(filename, root)
	if err != nil {
		return nil, nil, err
	}

	correlation.Canonicalize(cx)

	if err := correlation.Prune(fn, root, cx); err != nil {
		return nil, nil, err
	}

	if err := writer.Write(cx, newFilename); err != nil {
		return nil, nil, err
	}

	return packages(cx), references(cx), nil
}

func packages(cx *correlation.CorrelationState) []Package {
	// TODO - de-duplicate
	var packages []Package
	for id := range cx.ExportedMonikers {
		source := cx.MonikerData[id]
		packageInfo := cx.PackageInformationData[source.PackageInformationID]
		packages = append(packages, Package{
			Scheme:  source.Scheme,
			Name:    packageInfo.Name,
			Version: packageInfo.Version,
		})
	}

	return packages
}

func references(cx *correlation.CorrelationState) []Reference {
	references := map[string]Reference{}
	for id := range cx.ImportedMonikers {
		source := cx.MonikerData[id]
		packageInfo := cx.PackageInformationData[source.PackageInformationID]
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

	return refs
}
