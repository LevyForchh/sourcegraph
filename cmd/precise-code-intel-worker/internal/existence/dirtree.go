package existence

import (
	"path/filepath"
	"strings"
)

type DirTreeNode struct {
	Name     string
	Children []DirTreeNode
}

func makeTree(root string, documentPaths []string) DirTreeNode {
	directorySet := map[string]struct{}{}
	for _, path := range documentPaths {
		if dir := dirWithoutDot(filepath.Join(root, path)); !strings.HasPrefix(dir, "..") {
			directorySet[dir] = struct{}{}
		}
	}

	tree := DirTreeNode{}
	for dir := range directorySet {
		tree = insertPathSegmentsIntoNode(tree, strings.Split(dir, "/"))
	}

	return tree
}

func insertPathSegmentsIntoNode(n DirTreeNode, pathSegments []string) DirTreeNode {
	if len(pathSegments) == 0 {
		return n
	}

	for i, c := range n.Children {
		if c.Name == pathSegments[0] {
			n.Children[i] = insertPathSegmentsIntoNode(c, pathSegments[1:])
			return n
		}
	}

	newChild := DirTreeNode{pathSegments[0], nil}
	n.Children = append(n.Children, insertPathSegmentsIntoNode(newChild, pathSegments[1:]))
	return n
}
