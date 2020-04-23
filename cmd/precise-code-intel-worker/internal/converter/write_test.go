package converter

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jmoiron/sqlx"
	"github.com/sourcegraph/sourcegraph/internal/sqliteutil"
)

func init() {
	sqliteutil.SetLocalLibpath()
	sqliteutil.MustRegisterSqlite3WithPcre()
}

func TestWrite(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unexpected error creating temp directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	state := &CorrelationState{
		LsifVersion: "0.4.3",
		DocumentData: map[string]DocumentData{
			"d01": {URI: "foo.go", Contains: idSet{"r01": {}, "r02": {}, "r03": {}}},
			"d02": {URI: "bar.go", Contains: idSet{"r04": {}, "r05": {}, "r06": {}}},
			"d03": {URI: "baz.go", Contains: idSet{"r07": {}, "r08": {}, "r09": {}}},
		},
		RangeData: map[string]RangeData{
			"r01": {StartLine: 1, StartCharacter: 2, EndLine: 3, EndCharacter: 4, DefinitionResultID: "x01", MonikerIDs: idSet{"m01": {}, "m02": {}}},
			"r02": {StartLine: 2, StartCharacter: 3, EndLine: 4, EndCharacter: 5, ReferenceResultID: "x06", MonikerIDs: idSet{"m03": {}, "m04": {}}},
			"r03": {StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6, DefinitionResultID: "x02"},
			"r04": {StartLine: 4, StartCharacter: 5, EndLine: 6, EndCharacter: 7, ReferenceResultID: "x07"},
			"r05": {StartLine: 5, StartCharacter: 6, EndLine: 7, EndCharacter: 8, DefinitionResultID: "x03"},
			"r06": {StartLine: 6, StartCharacter: 7, EndLine: 8, EndCharacter: 9, HoverResultID: "x08"},
			"r07": {StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0, DefinitionResultID: "x04"},
			"r08": {StartLine: 8, StartCharacter: 9, EndLine: 0, EndCharacter: 1, HoverResultID: "x09"},
			"r09": {StartLine: 9, StartCharacter: 0, EndLine: 1, EndCharacter: 2, DefinitionResultID: "x05"},
		},
		DefinitionData: map[string]defaultIDSetMap{
			"x01": {"d01": {"r03": {}}, "d02": {"r04": {}}, "d03": {"r07": {}}},
			"x02": {"d01": {"r02": {}}, "d02": {"r05": {}}, "d03": {"r08": {}}},
			"x03": {"d01": {"r01": {}}, "d02": {"r06": {}}, "d03": {"r09": {}}},
			"x04": {"d01": {"r03": {}}, "d02": {"r05": {}}, "d03": {"r07": {}}},
			"x05": {"d01": {"r02": {}}, "d02": {"r06": {}}, "d03": {"r08": {}}},
		},
		ReferenceData: map[string]defaultIDSetMap{
			"x06": {"d01": {"r03": {}}, "d03": {"r07": {}, "r09": {}}},
			"x07": {"d01": {"r02": {}}, "d03": {"r07": {}, "r09": {}}},
		},
		HoverData: map[string]string{
			"x08": "foo",
			"x09": "bar",
		},
		MonikerData: map[string]MonikerData{
			"m01": {Kind: "import", Scheme: "scheme A", Identifier: "ident A", PackageInformationID: "p01"},
			"m02": {Kind: "import", Scheme: "scheme B", Identifier: "ident B"},
			"m03": {Kind: "export", Scheme: "scheme C", Identifier: "ident C", PackageInformationID: "p02"},
			"m04": {Kind: "export", Scheme: "scheme D", Identifier: "ident D"},
		},
		PackageInformationData: map[string]PackageInformationData{
			"p01": {Name: "pkg A", Version: "0.1.0"},
			"p02": {Name: "pkg B", Version: "1.2.3"},
		},
		ImportedMonikers: idSet{"m01": {}},
		ExportedMonikers: idSet{"m03": {}},
	}

	filename := filepath.Join(tempDir, "test.db")
	if err := Write(state, filename); err != nil {
		t.Fatalf("unexpected error writing database: %s", err)
	}

	databaseContents, err := slurpDatabase(filename)
	if err != nil {
		t.Fatalf("unexpected error reading database: %s", err)
	}

	if databaseContents.lsifVersion != "0.4.3" {
		t.Errorf("unexpected lsif version. want=%s have=%s", "0.4.3", databaseContents.lsifVersion)
	}
	if databaseContents.sourcegraphVersion != "0.1.0" {
		t.Errorf("unexpected sourcegraph version. want=%s have=%s", "0.1.0", databaseContents.sourcegraphVersion)
	}
	if databaseContents.numResultChunks != 1 {
		t.Errorf("unexpected num result chunks. want=%d have=%d", 1, databaseContents.numResultChunks)
	}

	expectedDocumentData := map[string]DocumentDatax{
		"foo.go": {
			Ranges: map[ID]RangeDatax{
				"r01": {StartLine: 1, StartCharacter: 2, EndLine: 3, EndCharacter: 4, DefinitionResultID: "x01", MonikerIDs: []ID{"m01", "m02"}},
				"r02": {StartLine: 2, StartCharacter: 3, EndLine: 4, EndCharacter: 5, ReferenceResultID: "x06", MonikerIDs: []ID{"m03", "m04"}},
				"r03": {StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6, DefinitionResultID: "x02"},
			},
			HoverResults: map[ID]string{},
			Monikers: map[ID]MonikerDatax{
				"m01": {Kind: "import", Scheme: "scheme A", Identifier: "ident A", PackageInformationID: "p01"},
				"m02": {Kind: "import", Scheme: "scheme B", Identifier: "ident B"},
				"m03": {Kind: "export", Scheme: "scheme C", Identifier: "ident C", PackageInformationID: "p02"},
				"m04": {Kind: "export", Scheme: "scheme D", Identifier: "ident D"},
			},
			PackageInformation: map[ID]PackageInformationDatax{
				"p01": {Name: "pkg A", Version: "0.1.0"},
				"p02": {Name: "pkg B", Version: "1.2.3"},
			},
		},
		"bar.go": {
			Ranges: map[ID]RangeDatax{
				"r04": {StartLine: 4, StartCharacter: 5, EndLine: 6, EndCharacter: 7, ReferenceResultID: "x07"},
				"r05": {StartLine: 5, StartCharacter: 6, EndLine: 7, EndCharacter: 8, DefinitionResultID: "x03"},
				"r06": {StartLine: 6, StartCharacter: 7, EndLine: 8, EndCharacter: 9, HoverResultID: "x08"},
			},
			HoverResults:       map[ID]string{"x08": "foo"},
			Monikers:           map[ID]MonikerDatax{},
			PackageInformation: map[ID]PackageInformationDatax{},
		},
		"baz.go": {
			Ranges: map[ID]RangeDatax{
				"r07": {StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0, DefinitionResultID: "x04"},
				"r08": {StartLine: 8, StartCharacter: 9, EndLine: 0, EndCharacter: 1, HoverResultID: "x09"},
				"r09": {StartLine: 9, StartCharacter: 0, EndLine: 1, EndCharacter: 2, DefinitionResultID: "x05"},
			},
			HoverResults:       map[ID]string{"x09": "bar"},
			Monikers:           map[ID]MonikerDatax{},
			PackageInformation: map[ID]PackageInformationDatax{},
		},
	}
	if diff := cmp.Diff(expectedDocumentData, databaseContents.documentData); diff != "" {
		t.Errorf("unexpected document data (-want +got):\n%s", diff)
	}

	expectedResultChunkData := map[int]ResultChunkDatax{
		0: {
			DocumentPaths: map[ID]string{
				"d01": "foo.go",
				"d02": "bar.go",
				"d03": "baz.go",
			},
			DocumentIDRangeIDs: map[ID][]DocumentIDRangeIDx{
				"x01": {
					{DocumentID: "d01", RangeID: "r03"},
					{DocumentID: "d02", RangeID: "r04"},
					{DocumentID: "d03", RangeID: "r07"},
				},
				"x02": {
					{DocumentID: "d01", RangeID: "r02"},
					{DocumentID: "d02", RangeID: "r05"},
					{DocumentID: "d03", RangeID: "r08"},
				},
				"x03": {
					{DocumentID: "d01", RangeID: "r01"},
					{DocumentID: "d02", RangeID: "r06"},
					{DocumentID: "d03", RangeID: "r09"},
				},
				"x04": {
					{DocumentID: "d01", RangeID: "r03"},
					{DocumentID: "d02", RangeID: "r05"},
					{DocumentID: "d03", RangeID: "r07"},
				},
				"x05": {
					{DocumentID: "d01", RangeID: "r02"},
					{DocumentID: "d02", RangeID: "r06"},
					{DocumentID: "d03", RangeID: "r08"},
				},
				"x06": {
					{DocumentID: "d01", RangeID: "r03"},
					{DocumentID: "d03", RangeID: "r07"},
					{DocumentID: "d03", RangeID: "r09"},
				},
				"x07": {
					{DocumentID: "d01", RangeID: "r02"},
					{DocumentID: "d03", RangeID: "r07"},
					{DocumentID: "d03", RangeID: "r09"},
				},
			},
		},
	}
	if diff := cmp.Diff(expectedResultChunkData, databaseContents.resultChunkData); diff != "" {
		t.Errorf("unexpected result chunk data (-want +got):\n%s", diff)
	}

	expectedDefinitions := []DefinitionReference{
		{Scheme: "scheme A", Identifier: "ident A", DocumentPath: "bar.go", StartLine: 4, StartCharacter: 5, EndLine: 6, EndCharacter: 7},
		{Scheme: "scheme B", Identifier: "ident B", DocumentPath: "bar.go", StartLine: 4, StartCharacter: 5, EndLine: 6, EndCharacter: 7},
		{Scheme: "scheme A", Identifier: "ident A", DocumentPath: "baz.go", StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0},
		{Scheme: "scheme B", Identifier: "ident B", DocumentPath: "baz.go", StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0},
		{Scheme: "scheme A", Identifier: "ident A", DocumentPath: "foo.go", StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6},
		{Scheme: "scheme B", Identifier: "ident B", DocumentPath: "foo.go", StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6},
	}
	if diff := cmp.Diff(expectedDefinitions, databaseContents.definitions); diff != "" {
		t.Errorf("unexpected definitions (-want +got):\n%s", diff)
	}

	expectedReferences := []DefinitionReference{
		{Scheme: "scheme C", Identifier: "ident C", DocumentPath: "baz.go", StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0},
		{Scheme: "scheme C", Identifier: "ident C", DocumentPath: "baz.go", StartLine: 9, StartCharacter: 0, EndLine: 1, EndCharacter: 2},
		{Scheme: "scheme D", Identifier: "ident D", DocumentPath: "baz.go", StartLine: 7, StartCharacter: 8, EndLine: 9, EndCharacter: 0},
		{Scheme: "scheme D", Identifier: "ident D", DocumentPath: "baz.go", StartLine: 9, StartCharacter: 0, EndLine: 1, EndCharacter: 2},
		{Scheme: "scheme C", Identifier: "ident C", DocumentPath: "foo.go", StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6},
		{Scheme: "scheme D", Identifier: "ident D", DocumentPath: "foo.go", StartLine: 3, StartCharacter: 4, EndLine: 5, EndCharacter: 6},
	}
	if diff := cmp.Diff(expectedReferences, databaseContents.references); diff != "" {
		t.Errorf("unexpected references (-want +got):\n%s", diff)
	}
}

//
//

type DatabaseContents struct {
	lsifVersion        string
	sourcegraphVersion string
	numResultChunks    int
	documentData       map[string]DocumentDatax
	resultChunkData    map[int]ResultChunkDatax
	definitions        []DefinitionReference
	references         []DefinitionReference
}

func slurpDatabase(filename string) (DatabaseContents, error) {
	db, err := sqlx.Open("sqlite3_with_pcre", filename)
	if err != nil {
		return DatabaseContents{}, err
	}

	lsifVersion, sourcegraphVersion, numResultChunks, err := slurpMeta(db)
	if err != nil {
		return DatabaseContents{}, err
	}

	documents, err := slurpDocuments(db)
	if err != nil {
		return DatabaseContents{}, err
	}

	resultChunks, err := slurpResultChunks(db)
	if err != nil {
		return DatabaseContents{}, err
	}

	definitions, err := slurpDefinitionReferences(db, "definitions")
	if err != nil {
		return DatabaseContents{}, err
	}

	references, err := slurpDefinitionReferences(db, "references")
	if err != nil {
		return DatabaseContents{}, err
	}

	return DatabaseContents{lsifVersion, sourcegraphVersion, numResultChunks, documents, resultChunks, definitions, references}, nil
}

func slurpMeta(db *sqlx.DB) (lsifVersion string, sourcegraphVersion string, numResultChunks int, err error) {
	query := `
		SELECT lsifVersion, sourcegraphVersion, numResultChunks
		FROM meta
		LIMIT 1
	`

	if err := db.QueryRow(query).Scan(&lsifVersion, &sourcegraphVersion, &numResultChunks); err != nil {
		return "", "", 0, err
	}

	return lsifVersion, sourcegraphVersion, numResultChunks, nil
}

func slurpDocuments(db *sqlx.DB) (map[string]DocumentDatax, error) {
	rows, err := db.Query(`SELECT path, data FROM documents`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documents := map[string]DocumentDatax{}
	for rows.Next() {
		var path string
		var rawData []byte
		if err := rows.Scan(&path, &rawData); err != nil {
			return nil, err
		}

		data, err := UnmarshalDocumentData(rawData)
		if err != nil {
			return nil, err
		}

		for _, r := range data.Ranges {
			sort.Slice(r.MonikerIDs, func(i, j int) bool {
				return strings.Compare(string(r.MonikerIDs[i]), string(r.MonikerIDs[j])) < 0
			})
		}

		documents[path] = data
	}

	return documents, nil
}

func slurpResultChunks(db *sqlx.DB) (map[int]ResultChunkDatax, error) {
	rows, err := db.Query(`SELECT id, data FROM resultChunks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultChunks := map[int]ResultChunkDatax{}
	for rows.Next() {
		var id int
		var rawData []byte
		if err := rows.Scan(&id, &rawData); err != nil {
			return nil, err
		}

		data, err := UnmarshalResultChunkData(rawData)
		if err != nil {
			return nil, err
		}

		for _, v := range data.DocumentIDRangeIDs {
			sort.Slice(v, func(i, j int) bool {
				if cmp := strings.Compare(string(v[i].DocumentID), string(v[j].DocumentID)); cmp != 0 {
					return cmp < 0
				}
				return strings.Compare(string(v[i].RangeID), string(v[j].RangeID)) < 0
			})
		}

		resultChunks[id] = data
	}

	return resultChunks, nil
}

type DefinitionReference struct {
	Scheme         string
	Identifier     string
	DocumentPath   string
	StartLine      int
	EndLine        int
	StartCharacter int
	EndCharacter   int
}

func slurpDefinitionReferences(db *sqlx.DB, tableName string) ([]DefinitionReference, error) {
	query := `
		SELECT scheme, identifier, documentPath, startLine, startCharacter, endLine, endCharacter
		FROM "%s"
	`

	rows, err := db.Query(fmt.Sprintf(query, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var definitionReferences []DefinitionReference
	for rows.Next() {
		var definitionReference DefinitionReference
		if err := rows.Scan(
			&definitionReference.Scheme,
			&definitionReference.Identifier,
			&definitionReference.DocumentPath,
			&definitionReference.StartLine,
			&definitionReference.StartCharacter,
			&definitionReference.EndLine,
			&definitionReference.EndCharacter,
		); err != nil {
			return nil, err
		}

		definitionReferences = append(definitionReferences, definitionReference)
	}

	sort.Slice(definitionReferences, func(i, j int) bool {
		if cmp := strings.Compare(definitionReferences[i].DocumentPath, definitionReferences[j].DocumentPath); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(definitionReferences[i].Identifier, definitionReferences[j].Identifier); cmp != 0 {
			return cmp < 0
		}

		return definitionReferences[i].StartLine < definitionReferences[j].StartLine
	})

	return definitionReferences, nil
}
