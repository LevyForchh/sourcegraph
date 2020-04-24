package correlation

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCorrelate(t *testing.T) {
	input, err := ioutil.ReadFile("../../testdata/dump.lsif")
	if err != nil {
		t.Fatalf("unexpected error reading test file: %s", err)
	}

	state, err := correlateFromReader("root", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error correlating input: %s", err)
	}

	expectedState := &CorrelationState{
		DumpRoot:            "root",
		LsifVersion:         "0.4.3",
		ProjectRoot:         "file:///test/root",
		UnsupportedVertexes: IDSet{},
		DocumentData: map[string]DocumentData{
			"02": {URI: "/foo.go", Contains: IDSet{"04": {}, "05": {}, "06": {}}},
			"03": {URI: "/bar.go", Contains: IDSet{"07": {}, "08": {}, "09": {}}},
		},
		RangeData: map[string]RangeData{
			"04": {
				StartLine:          1,
				StartCharacter:     2,
				EndLine:            3,
				EndCharacter:       4,
				DefinitionResultID: "13",
				MonikerIDs:         IDSet{},
			},
			"05": {
				StartLine:         2,
				StartCharacter:    3,
				EndLine:           4,
				EndCharacter:      5,
				ReferenceResultID: "15",
				MonikerIDs:        IDSet{},
			},
			"06": {
				StartLine:          3,
				StartCharacter:     4,
				EndLine:            5,
				EndCharacter:       6,
				DefinitionResultID: "13",
				HoverResultID:      "17",
				MonikerIDs:         IDSet{},
			},
			"07": {
				StartLine:         4,
				StartCharacter:    5,
				EndLine:           6,
				EndCharacter:      7,
				ReferenceResultID: "15",
				MonikerIDs:        IDSet{"18": {}},
			},
			"08": {
				StartLine:      5,
				StartCharacter: 6,
				EndLine:        7,
				EndCharacter:   8,
				HoverResultID:  "17",
				MonikerIDs:     IDSet{},
			},
			"09": {
				StartLine:      6,
				StartCharacter: 7,
				EndLine:        8,
				EndCharacter:   9,
				MonikerIDs:     IDSet{"19": {}},
			},
		},
		ResultSetData: map[string]ResultSetData{
			"10": {
				DefinitionResultID: "12",
				ReferenceResultID:  "14",
				MonikerIDs:         IDSet{"20": {}},
			},
			"11": {
				HoverResultID: "16",
				MonikerIDs:    IDSet{"21": {}},
			},
		},
		DefinitionData: map[string]DefaultIDSetMap{
			"12": {"03": {"07": {}}},
			"13": {"03": {"08": {}}},
		},
		ReferenceData: map[string]DefaultIDSetMap{
			"14": {"02": {"04": {}, "05": {}}},
			"15": {},
		},
		HoverData: map[string]string{
			"16": "```go\ntext A\n```",
			"17": "```go\ntext B\n```",
		},
		MonikerData: map[string]MonikerData{
			"18": {Kind: "import", Scheme: "scheme A", Identifier: "ident A", PackageInformationID: "22"},
			"19": {Kind: "export", Scheme: "scheme B", Identifier: "ident B", PackageInformationID: "23"},
			"20": {Kind: "import", Scheme: "scheme C", Identifier: "ident C", PackageInformationID: ""},
			"21": {Kind: "export", Scheme: "scheme D", Identifier: "ident D", PackageInformationID: ""},
		},
		PackageInformationData: map[string]PackageInformationData{
			"22": {Name: "pkg A", Version: "v0.1.0"},
			"23": {Name: "pkg B", Version: "v1.2.3"},
		},
		NextData: map[string]string{
			"09": "10",
			"10": "11",
		},
		ImportedMonikers:       IDSet{"18": {}},
		ExportedMonikers:       IDSet{"19": {}},
		LinkedMonikers:         DisjointIDSet{"19": {"21": {}}, "21": {"19": {}}},
		LinkedReferenceResults: DisjointIDSet{"14": {"15": {}}, "15": {"14": {}}},
	}

	if diff := cmp.Diff(expectedState, state); diff != "" {
		t.Errorf("unexpected state (-want +got):\n%s", diff)
	}
}
