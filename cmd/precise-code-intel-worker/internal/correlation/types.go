package correlation

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Element struct {
	ID    string `json:"id"` // TODO - string or int
	Type  string `json:"type"`
	Label string `json:"label"`
	Raw   json.RawMessage
}

func unmarshalElement(Raw []byte) (payload Element, err error) {
	err = json.Unmarshal(Raw, &payload)
	payload.Raw = json.RawMessage(Raw)
	return payload, err
}

//
//

type Edge struct {
	OutV     string   `json:"outV"`
	InV      string   `json:"inV"`
	InVs     []string `json:"inVs"`
	Document string   `json:"document"`
}

func unmarshalEdge(element Element) (payload Edge, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	return payload, err
}

//
//

type MetaData struct {
	Version     string `json:"version"`
	ProjectRoot string `json:"projectRoot"`
}

func unmarshalMetaData(element Element, dumpRoot string) (payload MetaData, err error) {
	err = json.Unmarshal(element.Raw, &payload)

	// We assume that the project root in the LSIF dump is either:
	//
	//   (1) the root of the LSIF dump, or
	//   (2) the root of the repository
	//
	// These are the common cases and we don't explicitly support
	// anything else. Here we normalize to (1) by appending the dump
	// root if it's not already suffixed by it.

	if !strings.HasSuffix(payload.ProjectRoot, "/") {
		payload.ProjectRoot += "/"
	}

	if dumpRoot != "" && !strings.HasPrefix(payload.ProjectRoot, dumpRoot) {
		payload.ProjectRoot += dumpRoot
	}

	return payload, err
}

//
//

type DocumentData struct {
	URI      string `json:"uri"`
	Contains IDSet
}

func unmarshalDocumentData(element Element, projectRoot string) (payload DocumentData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	if !strings.HasPrefix(payload.URI, projectRoot) {
		return DocumentData{}, fmt.Errorf("document URI %s is not relative to project root %s", payload.URI, projectRoot)
	}
	payload.URI = payload.URI[len(projectRoot):]
	payload.Contains = IDSet{}
	return payload, err
}

//
//

type RangeData struct {
	StartLine          int    `json:"startLine"`
	StartCharacter     int    `json:"startCharacter"`
	EndLine            int    `json:"endLine"`
	EndCharacter       int    `json:"endCharacter"`
	DefinitionResultID string `json:"definitionResultId"`
	ReferenceResultID  string `json:"referenceResultId"`
	HoverResultID      string `json:"hoverResultId"`
	MonikerIDs         IDSet  `json:"monikerIds"`
}

func unmarshalRangeData(element Element) (RangeData, error) {
	type Position struct {
		Line      int `json:"line"`
		Character int `json:"character"`
	}

	type RangeVertex struct {
		Start Position `json:"start"`
		End   Position `json:"end"`
	}

	var payload RangeVertex
	if err := json.Unmarshal(element.Raw, &payload); err != nil {
		return RangeData{}, err
	}

	return RangeData{
		StartLine:      payload.Start.Line,
		StartCharacter: payload.Start.Character,
		EndLine:        payload.End.Line,
		EndCharacter:   payload.End.Character,
		MonikerIDs:     IDSet{},
	}, nil
}

func (d RangeData) setDefinitionResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: id,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setReferenceResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  id,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setHoverResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      id,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setMonikerIDs(ids IDSet) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         ids,
	}
}

//
//

func unmarshalHoverData(element Element) (string, error) {
	type HoverResult struct {
		Contents json.RawMessage `json:"contents"`
	}
	type HoverVertex struct {
		Result HoverResult `json:"result"`
	}

	var payload HoverVertex
	if err := json.Unmarshal(element.Raw, &payload); err != nil {
		return "", err
	}

	return unmarshalHover(payload.Result.Contents)
}

func unmarshalHover(blah json.RawMessage) (string, error) {
	var target []json.RawMessage
	if err := json.Unmarshal(blah, &target); err != nil {
		return unmarshalHoverPart(blah)
	}

	var parts []string
	for _, t := range target {
		part, err := unmarshalHoverPart(t)
		if err != nil {
			return "", err
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, "\n\n---\n\n"), nil
}

func unmarshalHoverPart(blah json.RawMessage) (string, error) {
	var p string
	if err := json.Unmarshal(blah, &p); err == nil {
		return strings.TrimSpace(p), nil
	}

	var payload struct {
		Kind     string `json:"kind"`
		Language string `json:"language"`
		Value    string `json:"value"`
	}
	if err := json.Unmarshal(blah, &payload); err != nil {
		return "", fmt.Errorf("unrecognized hover format")
	}

	if payload.Language != "" {
		return fmt.Sprintf("```%s\n%s\n```", payload.Language, payload.Value), nil
	}

	return strings.TrimSpace(payload.Value), nil
}

//
//

type MonikerData struct {
	Kind                 string `json:"kind"`
	Scheme               string `json:"scheme"`
	Identifier           string `json:"identifier"`
	PackageInformationID string
}

func unmarshalMonikerData(element Element) (payload MonikerData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	if payload.Kind == "" {
		payload.Kind = "local"
	}
	return payload, err
}

func (d MonikerData) setPackageInformationID(id string) MonikerData {
	return MonikerData{
		Kind:                 d.Kind,
		Scheme:               d.Scheme,
		Identifier:           d.Identifier,
		PackageInformationID: id,
	}
}

//
//

type PackageInformationData struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func unmarshalPackageInformationData(element Element) (payload PackageInformationData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	return payload, err
}

//
//

type ResultSetData struct {
	DefinitionResultID string
	ReferenceResultID  string
	HoverResultID      string
	MonikerIDs         IDSet
}

func unmarshalResultSetData(element Element) (ResultSetData, error) {
	return ResultSetData{MonikerIDs: IDSet{}}, nil
}

func (d ResultSetData) setDefinitionResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: id,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setReferenceResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  id,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setHoverResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      id,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setMonikerIDs(ids IDSet) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         ids,
	}
}

//
//

func unmarshalDefinitionResultData(element Element) (DefaultIDSetMap, error) {
	return map[string]IDSet{}, nil
}

func unmarshalReferenceResultData(element Element) (DefaultIDSetMap, error) {
	return map[string]IDSet{}, nil
}