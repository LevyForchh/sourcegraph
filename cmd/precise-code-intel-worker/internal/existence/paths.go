package existence

import "path/filepath"

func dirWithoutDot(path string) string {
	if dir := filepath.Dir(path); dir != "." {
		return dir
	}
	return ""
}
