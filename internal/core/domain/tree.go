package domain

import (
	"fmt"
	"sort"
	"strings"
)

const (
	KindSource NodeKind = "source"
	KindStage  NodeKind = "stage"
	KindSink   NodeKind = "sink"
)

type NodeKind string

func (k NodeKind) Value() int {
	switch k {
	case KindSource:
		return 0
	case KindStage:
		return 1
	case KindSink:
		return 2
	default:
		return -1
	}
}

type Tree struct {
	treeMap map[string]*Node
	root    *Node
}

type Node struct {
	Name     string
	Kind     NodeKind
	Children []*Node
}

func NewTree(s StreamSpec) (*Tree, error) {
	treeMap := make(map[string]*Node)
	root := &Node{
		Children: []*Node{},
		Kind:     KindSource,
		Name:     s.Source.Name,
	}
	treeMap[s.Source.Name] = root

	for _, stage := range s.Stages {
		if _, ok := treeMap[stage.Name]; ok {
			return nil, ErrStageDuplicate
		}
		treeMap[stage.Name] = &Node{
			Children: []*Node{},
			Kind:     KindStage,
			Name:     stage.Name,
		}
	}

	for _, stage := range s.Stages {
		from, ok := treeMap[stage.From]
		if !ok {
			return nil, ErrStageInvalidFrom
		}

		node := treeMap[stage.Name]
		treeMap[stage.From].Children = append(from.Children, node)
	}

	for _, sink := range s.Sinks {
		if _, ok := treeMap[sink.Name]; ok {
			return nil, ErrSinkDuplicate
		}
		from, ok := treeMap[sink.From]
		if !ok {
			return nil, ErrSinkInvalidFrom
		}
		node := &Node{
			Children: []*Node{},
			Kind:     KindSink,
			Name:     sink.Name,
		}
		treeMap[sink.Name] = node
		from.Children = append(from.Children, node)
	}

	tree := Tree{
		treeMap: treeMap,
		root:    root,
	}
	return &tree, nil
}

func (g Tree) IsTree() bool {
	if len(g.treeMap) == 0 {
		return true
	}

	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	for node := range g.treeMap {
		if !visited[node] {
			if g.isCyclic(node, visited, recursionStack) {
				return false
			}
		}
	}
	return true
}

func (g Tree) isCyclic(name string, visited map[string]bool, recursionStack map[string]bool) bool {
	visited[name] = true
	recursionStack[name] = true

	node, ok := g.treeMap[name]
	if !ok {
		return false
	}

	for _, neighbor := range node.Children {
		if !visited[neighbor.Name] {
			if g.isCyclic(neighbor.Name, visited, recursionStack) {
				return true
			}
		} else if recursionStack[neighbor.Name] {
			return true
		}
	}

	recursionStack[name] = false
	return false
}

func (g Tree) IsConnected() bool {
	if len(g.treeMap) == 0 {
		return true
	}

	visited := make(map[string]bool)
	startNode := ""
	for name, node := range g.treeMap {
		if node.Kind == KindSource {
			startNode = name
			break
		}
	}

	g.dfs(startNode, visited)

	return len(visited) == len(g.treeMap)
}

func (g Tree) dfs(name string, visited map[string]bool) {
	visited[name] = true

	node, ok := g.treeMap[name]
	if !ok {
		return
	}

	for _, neighbor := range node.Children {
		if !visited[neighbor.Name] {
			g.dfs(neighbor.Name, visited)
		}
	}
}

func (g Tree) Root() *Node {
	return g.root
}

func (g Tree) LeafNodes() []*Node {
	var leafNodes []*Node
	for _, node := range g.treeMap {
		if len(node.Children) == 0 {
			leafNodes = append(leafNodes, node)
		}
	}
	return leafNodes
}

func (g Tree) String() string {
	source := g.Root()
	if source == nil {
		return Node{}.String()
	}
	return source.String()
}

func (n Node) String() string {
	var children []*Node
	for _, child := range n.Children {
		children = append(children, child)
	}
	sort.Slice(children, func(i, j int) bool {
		return children[i].Kind.Value() < children[j].Kind.Value()
	})

	var childStrings []string
	for _, child := range children {
		childStrings = append(childStrings, child.String())
	}

	return fmt.Sprintf("%s:%s->[%s]", n.Kind, n.Name, strings.Join(childStrings, ","))
}

func (t Tree) Map() map[string]*Node {
	return t.treeMap
}
