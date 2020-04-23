package existence

import (
	"path/filepath"
	"sort"
)

type ExistenceChecker struct {
	root              string
	directoryContents map[string]map[string]struct{}
}

// TODO - rename
type GetChildrenFunc func(dirnames []string) (map[string][]string, error)

func NewExistenceChecker(root string, paths []string, fn GetChildrenFunc) (*ExistenceChecker, error) {
	directoryContents, err := directoryContents(root, paths, fn)
	if err != nil {
		return nil, err
	}

	return &ExistenceChecker{root, directoryContents}, nil
}

func (ec *ExistenceChecker) ShouldInclude(path string) bool {
	if children, ok := ec.directoryContents[dirWithoutDot(filepath.Join(ec.root, path))]; ok {
		if _, ok := children[path]; ok {
			return true
		}
	}

	return false
}

// TODO - rename fn
func directoryContents(root string, paths []string, fn GetChildrenFunc) (map[string]map[string]struct{}, error) {
	contents := map[string]map[string]struct{}{}

	for batch := makeInitialRequestBatch(root, paths); len(batch) > 0; batch = batch.next(contents) {
		batchResults, err := fn(batch.dirnames())
		if err != nil {
			return nil, err
		}

		for directory, children := range batchResults {
			if len(children) > 0 {
				v := map[string]struct{}{}
				for _, c := range children {
					v[c] = struct{}{}
				}
				contents[directory] = v
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

func (batch RequestBatch) next(contents map[string]map[string]struct{}) RequestBatch {
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
