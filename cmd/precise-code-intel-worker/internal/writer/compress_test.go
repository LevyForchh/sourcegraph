package writer

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGzipJSON(t *testing.T) {
	values := map[string]interface{}{
		"foo": true,
		"bar": float64(123),
		"baz": []interface{}{"bonk", "quux"},
	}

	compressed, err := gzipJSON(values)
	if err != nil {
		t.Fatalf("unexpected error compressing data: %s", err)
	}

	gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("unexpected error unzipping compressed data: %s", err)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(gzipReader).Decode(&payload); err != nil {
		t.Fatalf("unexpected error unmarshalling data: %s", err)
	}

	if diff := cmp.Diff(values, payload); diff != "" {
		t.Errorf("unexpected data (-want +got):\n%s", diff)
	}
}
