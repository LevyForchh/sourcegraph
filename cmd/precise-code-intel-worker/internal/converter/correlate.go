package converter

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
)

type CorrelationState struct {
	DumpRoot               string
	LsifVersion            string
	ProjectRoot            string
	UnsupportedVertexes    idSet
	DocumentData           map[string]DocumentData
	RangeData              map[string]RangeData
	ResultSetData          map[string]ResultSetData
	DefinitionData         map[string]defaultIDSetMap
	ReferenceData          map[string]defaultIDSetMap
	HoverData              map[string]string
	MonikerData            map[string]MonikerData
	PackageInformationData map[string]PackageInformationData
	NextData               map[string]string
	ImportedMonikers       idSet
	ExportedMonikers       idSet
	LinkedMonikers         disjointIDSet
	LinkedReferenceResults disjointIDSet
}

func newCorrelationState(dumpRoot string) *CorrelationState {
	return &CorrelationState{
		DumpRoot:               dumpRoot,
		UnsupportedVertexes:    idSet{},
		DocumentData:           map[string]DocumentData{},
		RangeData:              map[string]RangeData{},
		ResultSetData:          map[string]ResultSetData{},
		DefinitionData:         map[string]defaultIDSetMap{},
		ReferenceData:          map[string]defaultIDSetMap{},
		HoverData:              map[string]string{},
		MonikerData:            map[string]MonikerData{},
		PackageInformationData: map[string]PackageInformationData{},
		NextData:               map[string]string{},
		ImportedMonikers:       idSet{},
		ExportedMonikers:       idSet{},
		LinkedMonikers:         disjointIDSet{},
		LinkedReferenceResults: disjointIDSet{},
	}
}

var ErrMissingMetaData = errors.New("no metadata defined")

type ErrMalformedDump struct {
	ID         string
	References string
	Kinds      []string
}

func (e ErrMalformedDump) Error() string {
	// TODO
	return fmt.Sprintf("oh geesh: %s %s %v", e.ID, e.References, e.Kinds)
}

func malformedDump(id, references string, kinds ...string) error {
	return ErrMalformedDump{
		ID:         id,
		References: references,
		Kinds:      kinds,
	}
}

func correlate(filename, root string) (*CorrelationState, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	return correlateFromReader(root, gzipReader)
}

func correlateFromReader(root string, r io.Reader) (*CorrelationState, error) {
	cx := newCorrelationState(root)
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		bx := scanner.Bytes()
		element, err := unmarshalElement(bx)
		if err != nil {
			return nil, err
		}

		if err := correlateElement(cx, element); err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if cx.LsifVersion == "" {
		return nil, fmt.Errorf("no metadata defined")
	}

	return cx, nil
}

func correlateElement(state *CorrelationState, element Element) error {
	switch element.Type {
	case "vertex":
		return correlateVertex(state, element)
	case "edge":
		return correlateEdge(state, element)
	}

	return fmt.Errorf("unknown element type %s", element.Type)
}

func correlateVertex(state *CorrelationState, element Element) error {
	handler, ok := vertexHandlers[element.Label]
	if !ok {
		state.UnsupportedVertexes.add(element.ID)
		return nil
	}

	return handler(state, element)
}

var vertexHandlers = map[string]func(c *CorrelationState, element Element) error{
	"metaData":           correlateMetaData,
	"document":           correlateDocument,
	"range":              correlateRange,
	"resultSet":          correlateResultSet,
	"definitionResult":   correlateDefinitionResult,
	"referenceResult":    correlateReferenceResult,
	"hoverResult":        correlateHoverResult,
	"moniker":            correlateMoniker,
	"packageInformation": correlatePackageInformation,
}

func correlateEdge(state *CorrelationState, element Element) error {
	handler, ok := edgeHandlers[element.Label]
	if !ok {
		return nil
	}

	edge, err := unmarshalEdge(element)
	if err != nil {
		return err
	}

	return handler(state, element.ID, edge)
}

var edgeHandlers = map[string]func(c *CorrelationState, id string, edge Edge) error{
	"contains":                correlateContainsEdge,
	"next":                    correlateNextEdge,
	"item":                    correlateItemEdge,
	"textDocument/definition": correlateTextDocumentDefinitionEdge,
	"textDocument/references": correlateTextDocumentReferencesEdge,
	"textDocument/hover":      correlateTextDocumentHoverEdge,
	"moniker":                 correlateMonikerEdge,
	"nextMoniker":             correlateNextMonikerEdge,
	"packageInformation":      correlatePackageInformationEdge,
}

func correlateMetaData(c *CorrelationState, element Element) error {
	payload, err := unmarshalMetaData(element, c.DumpRoot)
	c.LsifVersion = payload.Version
	c.ProjectRoot = payload.ProjectRoot
	return err
}

func correlateDocument(c *CorrelationState, element Element) error {
	if c.ProjectRoot == "" {
		return ErrMissingMetaData
	}

	payload, err := unmarshalDocumentData(element, c.ProjectRoot)
	c.DocumentData[element.ID] = payload
	return err
}

func correlateRange(c *CorrelationState, element Element) error {
	payload, err := unmarshalRangeData(element)
	c.RangeData[element.ID] = payload
	return err
}

func correlateResultSet(c *CorrelationState, element Element) error {
	payload, err := unmarshalResultSetData(element)
	c.ResultSetData[element.ID] = payload
	return err
}

func correlateDefinitionResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalDefinitionResultData(element)
	c.DefinitionData[element.ID] = payload
	return err
}

func correlateReferenceResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalReferenceResultData(element)
	c.ReferenceData[element.ID] = payload
	return err
}

func correlateHoverResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalHoverData(element)
	c.HoverData[element.ID] = payload
	return err
}

func correlateMoniker(c *CorrelationState, element Element) error {
	payload, err := unmarshalMonikerData(element)
	c.MonikerData[element.ID] = payload
	return err
}

func correlatePackageInformation(c *CorrelationState, element Element) error {
	payload, err := unmarshalPackageInformationData(element)
	c.PackageInformationData[element.ID] = payload
	return err
}

func correlateContainsEdge(c *CorrelationState, id string, edge Edge) error {
	doc, ok := c.DocumentData[edge.OutV]
	if !ok {
		// Do not track project contains
		return nil
	}

	for _, inV := range edge.InVs {
		if _, ok := c.RangeData[inV]; !ok {
			return malformedDump(id, edge.InV, "range")
		}
		doc.Contains.add(inV)
	}
	return nil
}

func correlateNextEdge(c *CorrelationState, id string, edge Edge) error {
	_, ok1 := c.RangeData[edge.OutV]
	_, ok2 := c.ResultSetData[edge.OutV]
	if !ok1 && !ok2 {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	if _, ok := c.ResultSetData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "resultSet")
	}

	c.NextData[edge.OutV] = edge.InV
	return nil
}

func correlateItemEdge(c *CorrelationState, id string, edge Edge) error {
	if documentMap, ok := c.DefinitionData[edge.OutV]; ok {
		for _, inV := range edge.InVs {
			if _, ok := c.RangeData[inV]; !ok {
				return malformedDump(id, edge.InV, "range")
			}
			documentMap.getOrCreate(edge.Document).add(inV)
		}

		return nil
	}

	if documentMap, ok := c.ReferenceData[edge.OutV]; ok {
		for _, inV := range edge.InVs {
			if _, ok := c.ReferenceData[inV]; ok {
				c.LinkedReferenceResults.union(edge.OutV, inV)
			} else {
				if _, ok = c.RangeData[inV]; !ok {
					return malformedDump(id, edge.InV, "range")
				}
				documentMap.getOrCreate(edge.Document).add(inV)
			}
		}

		return nil
	}

	if !c.UnsupportedVertexes.contains(edge.OutV) {
		return malformedDump(id, edge.OutV, "vertex")
	}

	// this.logger.debug("Skipping edge from an unsupported vertex")
	return nil
}

func correlateTextDocumentDefinitionEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.DefinitionData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "definitionResult")
	}

	if source, ok := c.RangeData[edge.OutV]; ok {
		c.RangeData[edge.OutV] = source.setDefinitionResultID(edge.InV)
	} else if source, ok := c.ResultSetData[edge.OutV]; ok {
		c.ResultSetData[edge.OutV] = source.setDefinitionResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateTextDocumentReferencesEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.ReferenceData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "referenceResult")
	}

	if source, ok := c.RangeData[edge.OutV]; ok {
		c.RangeData[edge.OutV] = source.setReferenceResultID(edge.InV)
	} else if source, ok := c.ResultSetData[edge.OutV]; ok {
		c.ResultSetData[edge.OutV] = source.setReferenceResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateTextDocumentHoverEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.HoverData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "hoverResult")
	}

	if source, ok := c.RangeData[edge.OutV]; ok {
		c.RangeData[edge.OutV] = source.setHoverResultID(edge.InV)
	} else if source, ok := c.ResultSetData[edge.OutV]; ok {
		c.ResultSetData[edge.OutV] = source.setHoverResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateMonikerEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.MonikerData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "moniker")
	}

	ids := idSet{}
	ids.add(edge.InV)

	if source, ok := c.RangeData[edge.OutV]; ok {
		c.RangeData[edge.OutV] = source.setMonikerIDs(ids)
	} else if source, ok := c.ResultSetData[edge.OutV]; ok {
		c.ResultSetData[edge.OutV] = source.setMonikerIDs(ids)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateNextMonikerEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.MonikerData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "moniker")
	}
	if _, ok := c.MonikerData[edge.OutV]; !ok {
		return malformedDump(id, edge.OutV, "moniker")
	}

	c.LinkedMonikers.union(edge.InV, edge.OutV)
	return nil
}

func correlatePackageInformationEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.PackageInformationData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "packageInformation")
	}

	source, ok := c.MonikerData[edge.OutV]
	if !ok {
		return malformedDump(id, edge.OutV, "moniker")
	}

	switch source.Kind {
	case "import":
		c.ImportedMonikers.add(edge.OutV)
	case "export":
		c.ExportedMonikers.add(edge.OutV)
	}

	c.MonikerData[edge.OutV] = source.setPackageInformationID(edge.InV)
	return nil
}
