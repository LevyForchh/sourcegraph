package writer

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
)

func gzipJSON(payload interface{}) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
