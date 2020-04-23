package converter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strconv"
)

// hashKey hashes a string identifier into the range [0, maxIndex)`. The
// hash algorithm here is similar ot the one used in Java's String.hashCode.
// This implementation is identical to the TypeScript version used before
// the port to Go so that we can continue to read old conversions without
// a migration.
func hashKey(id string, maxIndex int) int {
	hash := int32(0)
	for _, c := range string(id) {
		hash = (hash << 5) - hash + int32(c)
	}

	if hash < 0 {
		hash = -hash
	}

	return int(hash % int32(maxIndex))
}

type ID string

// DocumentData represents a single document within an index. The data here can answer
// definitions, references, and hover queries if the results are all contained in the
// same document.
type DocumentDatax struct {
	Ranges             map[ID]RangeDatax
	HoverResults       map[ID]string // hover text normalized to markdown string
	Monikers           map[ID]MonikerDatax
	PackageInformation map[ID]PackageInformationDatax
}

// RangeData represents a range vertex within an index. It contains the same relevant
// edge data, which can be subsequently queried in the containing document. The data
// that was reachable via a result set has been collapsed into this object during
// conversion.
type RangeDatax struct {
	StartLine          int  // 0-indexed, inclusive
	StartCharacter     int  // 0-indexed, inclusive
	EndLine            int  // 0-indexed, inclusive
	EndCharacter       int  // 0-indexed, inclusive
	DefinitionResultID ID   // possibly empty
	ReferenceResultID  ID   // possibly empty
	HoverResultID      ID   // possibly empty
	MonikerIDs         []ID // possibly empty
}

// MonikerData represent a unique name (eventually) attached to a range.
type MonikerDatax struct {
	Kind                 string `json:"kind"`                 // local, import, export
	Scheme               string `json:"scheme"`               // name of the package manager type
	Identifier           string `json:"identifier"`           // unique identifier
	PackageInformationID ID     `json:"packageInformationId"` // possibly empty
}

// PackageInformationData indicates a globally unique namespace for a moniker.
type PackageInformationDatax struct {
	// Name of the package that contains the moniker.
	Name string

	// Version of the package.
	Version string
}

// ResultChunkData represents a row of the resultChunk table. Each row is a subset
// of definition and reference result data in the index. Results are inserted into
// chunks based on the hash of their identifier, thus every chunk has a roughly
// proportional amount of data.
type ResultChunkDatax struct {
	// DocumentPaths is a mapping from document identifiers to their paths. This
	// must be used to convert a document identifier in DocumentIDRangeIDs into
	// a key that can be used to fetch document data.
	DocumentPaths map[ID]string

	// DocumentIDRangeIDs is a mapping from a definition or result reference
	// identifier to the set of ranges that compose that result set. Each range
	// is paired with the identifier of the document in which it can found.
	DocumentIDRangeIDs map[ID][]DocumentIDRangeIDx
}

// DocumentIDRangeID is a pair of document and range identifiers.
type DocumentIDRangeIDx struct {
	// The identifier of the document to which the range belongs. This id is only
	// relevant within the containing result chunk.
	DocumentID ID

	// The identifier of the range.
	RangeID ID
}

// wrappedMapValue represents a JSON-encoded map with the following form.
// This maintains the same functionality that exists on the TypeScript side.
//
//     {
//       "value": [
//         ["key-1", "value-1"],
//         ["key-2", "value-2"],
//         ...
//       ]
//     }
type wrappedMapValue struct {
	Value []json.RawMessage `json:"value"`
}

// wrappedSetValue represents a JSON-encoded set with the following form.
// This maintains the same functionality that exists on the TypeScript side.
//
//     {
//       "value": [
//         "value-1",
//         "value-2",
//         ...
//       ]
//     }
type wrappedSetValue struct {
	Value []json.RawMessage `json:"value"`
}

// UnmarshalJSON converts a JSON number or string into an identifier. This
// maintains the same functionality that exists on the TypeScript side by
// simply running JSON.parse() on document and result chunk data blobs.
func (id *ID) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		return json.Unmarshal(b, (*string)(id))
	}

	var value int64
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}

	*id = ID(strconv.FormatInt(value, 10))
	return nil
}

// UnmarshalDocumentData unmarshals document data from a gzipped json-encoded blob.
func UnmarshalDocumentData(data []byte) (DocumentDatax, error) {
	payload := struct {
		Ranges             wrappedMapValue `json:"ranges"`
		HoverResults       wrappedMapValue `json:"hoverResults"`
		Monikers           wrappedMapValue `json:"monikers"`
		PackageInformation wrappedMapValue `json:"packageInformation"`
	}{}

	if err := unmarshalGzippedJSON(data, &payload); err != nil {
		return DocumentDatax{}, err
	}

	ranges, err := unmarshalWrappedRanges(payload.Ranges.Value)
	if err != nil {
		return DocumentDatax{}, err
	}

	hoverResults, err := unmarshalWrappedHoverResults(payload.HoverResults.Value)
	if err != nil {
		return DocumentDatax{}, err
	}

	monikers, err := unmarshalWrappedMonikers(payload.Monikers.Value)
	if err != nil {
		return DocumentDatax{}, err
	}

	packageInformation, err := unmarshalWrappedPackageInformation(payload.PackageInformation.Value)
	if err != nil {
		return DocumentDatax{}, err
	}

	return DocumentDatax{
		Ranges:             ranges,
		HoverResults:       hoverResults,
		Monikers:           monikers,
		PackageInformation: packageInformation,
	}, nil
}

// UnmarshalDocumentData unmarshals result chunk data from a gzipped json-encoded blob.
func UnmarshalResultChunkData(data []byte) (ResultChunkDatax, error) {
	payload := struct {
		DocumentPaths      wrappedMapValue `json:"documentPaths"`
		DocumentIDRangeIDs wrappedMapValue `json:"documentIdRangeIds"`
	}{}

	if err := unmarshalGzippedJSON(data, &payload); err != nil {
		return ResultChunkDatax{}, err
	}

	documentPaths, err := unmarshalWrappedDocumentPaths(payload.DocumentPaths.Value)
	if err != nil {
		return ResultChunkDatax{}, err
	}

	documentIDRangeIDs, err := unmarshalWrappedDocumentIdRangeIDs(payload.DocumentIDRangeIDs.Value)
	if err != nil {
		return ResultChunkDatax{}, err
	}

	return ResultChunkDatax{
		DocumentPaths:      documentPaths,
		DocumentIDRangeIDs: documentIDRangeIDs,
	}, nil
}

func unmarshalWrappedRanges(pairs []json.RawMessage) (map[ID]RangeDatax, error) {
	m := map[ID]RangeDatax{}
	for _, pair := range pairs {
		var id ID
		var value struct {
			StartLine          int             `json:"startLine"`
			StartCharacter     int             `json:"startCharacter"`
			EndLine            int             `json:"endLine"`
			EndCharacter       int             `json:"endCharacter"`
			DefinitionResultID ID              `json:"definitionResultID"`
			ReferenceResultID  ID              `json:"referenceResultID"`
			HoverResultID      ID              `json:"hoverResultID"`
			MonikerIDs         wrappedSetValue `json:"monikerIDs"`
		}

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		var monikerIDs []ID
		for _, value := range value.MonikerIDs.Value {
			var id ID
			if err := json.Unmarshal([]byte(value), &id); err != nil {
				return nil, err
			}

			monikerIDs = append(monikerIDs, id)
		}

		m[id] = RangeDatax{
			StartLine:          value.StartLine,
			StartCharacter:     value.StartCharacter,
			EndLine:            value.EndLine,
			EndCharacter:       value.EndCharacter,
			DefinitionResultID: value.DefinitionResultID,
			ReferenceResultID:  value.ReferenceResultID,
			HoverResultID:      value.HoverResultID,
			MonikerIDs:         monikerIDs,
		}
	}

	return m, nil
}

func unmarshalWrappedHoverResults(pairs []json.RawMessage) (map[ID]string, error) {
	m := map[ID]string{}
	for _, pair := range pairs {
		var id ID
		var value string

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		m[id] = value
	}

	return m, nil
}

func unmarshalWrappedMonikers(pairs []json.RawMessage) (map[ID]MonikerDatax, error) {
	m := map[ID]MonikerDatax{}
	for _, pair := range pairs {
		var id ID
		var value struct {
			Kind                 string `json:"kind"`
			Scheme               string `json:"scheme"`
			Identifier           string `json:"identifier"`
			PackageInformationID ID     `json:"packageInformationID"`
		}

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		m[id] = MonikerDatax{
			Kind:                 value.Kind,
			Scheme:               value.Scheme,
			Identifier:           value.Identifier,
			PackageInformationID: value.PackageInformationID,
		}
	}

	return m, nil
}

func unmarshalWrappedPackageInformation(pairs []json.RawMessage) (map[ID]PackageInformationDatax, error) {
	m := map[ID]PackageInformationDatax{}
	for _, pair := range pairs {
		var id ID
		var value struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		}

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		m[id] = PackageInformationDatax{
			Name:    value.Name,
			Version: value.Version,
		}
	}

	return m, nil
}

func unmarshalWrappedDocumentPaths(pairs []json.RawMessage) (map[ID]string, error) {
	m := map[ID]string{}
	for _, pair := range pairs {
		var id ID
		var value string

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		m[id] = value
	}

	return m, nil
}

func unmarshalWrappedDocumentIdRangeIDs(pairs []json.RawMessage) (map[ID][]DocumentIDRangeIDx, error) {
	m := map[ID][]DocumentIDRangeIDx{}
	for _, pair := range pairs {
		var id ID
		var value []struct {
			DocumentID ID `json:"documentId"`
			RangeID    ID `json:"rangeId"`
		}

		target := []interface{}{&id, &value}
		if err := json.Unmarshal([]byte(pair), &target); err != nil {
			return nil, err
		}

		var documentIDRangeIDs []DocumentIDRangeIDx
		for _, v := range value {
			documentIDRangeIDs = append(documentIDRangeIDs, DocumentIDRangeIDx{
				DocumentID: v.DocumentID,
				RangeID:    v.RangeID,
			})
		}

		m[id] = documentIDRangeIDs
	}

	return m, nil
}

// unmarshalGzippedJSON unmarshals the gzip+json encoded data.
func unmarshalGzippedJSON(data []byte, payload interface{}) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}

	return json.NewDecoder(gzipReader).Decode(&payload)
}
