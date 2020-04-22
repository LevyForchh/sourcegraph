package existence

import (
	"path/filepath"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
)

type ExistenceChecker struct {
	root        string
	dirContents map[string][]string
}

func NewExistenceChecker(db db.DB, repositoryID int, commit, root string, paths []string) (*ExistenceChecker, error) {
	dirContents, err := getDirectoryContents(db, repositoryID, commit, root, paths)
	if err != nil {
		return nil, err
	}

	return &ExistenceChecker{root, dirContents}, nil
}

func (ec *ExistenceChecker) ShouldInclude(path string) bool {
	relative := filepath.Join(ec.root, path)
	if children, ok := ec.dirContents[dirnameWithoutDot(relative)]; !ok || !includes(children, path) {
		return false
	}

	return true
}

type xbatch struct {
	parent   string
	children []Node
}

func getDirectoryContents(db db.DB, repositoryID int, commit, root string, paths []string) (map[string][]string, error) {
	batch := []xbatch{
		{"", []Node{makeTree(root, paths)}},
	}

	contents := map[string][]string{}
	for len(batch) > 0 {


		names := []string{}
		for _, x := range batch {
			for _, c := range x.children {
				names = append(names, filepath.Join(x.parent, c.dirname))
			}
		}

		children, err := getDirectoryChildren(db, repositoryID, commit, names)
		if err != nil {
			return nil, err
		}

		// TODO - something wrong around here

		for k, v := range children {
			contents[k] = v
		}


		var newBatch []xbatch
		for _, x := range batch {

			if xxx, ok := contents[x.parent]; !ok || len(xxx) == 0 {
				continue
			}

			for _, c := range x.children {
				newBatch = append(newBatch, xbatch{filepath.Join(x.parent, c.dirname), c.children})
			}
		}
		batch = newBatch
	}

	return contents, nil
}

// TODO - make a set instead
func includes(l []string, p string) bool {
	for _, x := range l {
		if x == p {
			return true
		}
	}

	return false
}
