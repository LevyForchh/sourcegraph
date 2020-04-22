package existence

import (
	"path/filepath"
	"sort"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
)

type ExistenceChecker struct {
	root        string
	dirContents map[string][]string
}

func NewExistenceChecker(db db.DB, repositoryID int, commit, root string, paths []string) (*ExistenceChecker, error) {
	dirContents, err := getDirectoryContents(root, paths, func(dirnames []string) (map[string][]string, error) {
		return getDirectoryChildren(db, repositoryID, commit, paths)
	})
	if err != nil {
		return nil, err
	}

	return &ExistenceChecker{root, dirContents}, nil
}

func (ec *ExistenceChecker) ShouldInclude(path string) bool {
	// TODO - use a set instead
	includes := func(l []string, p string) bool {
		for _, x := range l {
			if x == p {
				return true
			}
		}

		return false
	}

	relative := filepath.Join(ec.root, path)
	if children, ok := ec.dirContents[dirWithoutDot(relative)]; !ok || !includes(children, path) {
		return false
	}

	return true
}

// TODO - real dumb way to do this
type getChildrenFunc func(dirnames []string) (map[string][]string, error)

// TODO - rename fn
func getDirectoryContents(root string, paths []string, fn getChildrenFunc) (map[string][]string, error) {
	contents := map[string][]string{}

	for batch := makeInitialRequestBatch(root, paths); len(batch) > 0; batch = batch.next(contents) {
		batchResults, err := fn(batch.dirnames())
		if err != nil {
			return nil, err
		}

		for directory, children := range batchResults {
			if len(children) > 0 {
				contents[directory] = children
			}
		}
	}

	return contents, nil
}

type RequestBatch map[string][]DirTreeNode

func makeInitialRequestBatch(root string, paths []string) RequestBatch {
	node := makeTree(root, paths)
	if root != "" {
		return RequestBatch{"": node.Children}
	}
	return RequestBatch{"": []DirTreeNode{node}}
}

func (batch RequestBatch) dirnames() []string {
	var dirnames []string
	for nodeGroupParentPath, nodes := range batch {
		for _, node := range nodes {
			dirnames = append(dirnames, filepath.Join(nodeGroupParentPath, node.Name))
		}
	}
	sort.Strings(dirnames)

	return dirnames
}

func (batch RequestBatch) next(contents map[string][]string) RequestBatch {
	nextBatch := RequestBatch{}
	for nodeGroupPath, nodes := range batch {
		for _, node := range nodes {
			nodePath := filepath.Join(nodeGroupPath, node.Name)

			if len(node.Children) > 0 && len(contents[nodePath]) > 0 {
				nextBatch[nodePath] = node.Children
			}
		}
	}
	return nextBatch
}
