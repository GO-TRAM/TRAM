package domain

const (
	KindSource = "source"
	KindStage  = "stage"
	KindSink   = "sink"
)

type Graph map[string]*Node

type Node struct {
	Name     string
	Kind     string
	Children []Child
}

type Child struct {
	Name string
	Kind string
}

func (g Graph) IsDAG() bool {
	if len(g) == 0 {
		return true
	}

	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	for node := range g {
		if !visited[node] {
			if g.isCyclic(node, visited, recursionStack) {
				return false
			}
		}
	}
	return true
}

func (g Graph) isCyclic(name string, visited map[string]bool, recursionStack map[string]bool) bool {
	visited[name] = true
	recursionStack[name] = true

	node, ok := g[name]
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

func (g Graph) IsConnected() bool {
	if len(g) == 0 {
		return true
	}

	visited := make(map[string]bool)
	startNode := ""
	for name, node := range g {
		if node.Kind == KindSource {
			startNode = name
			break
		}
	}

	g.dfs(startNode, visited)

	return len(visited) == len(g)
}

func (g Graph) dfs(name string, visited map[string]bool) {
	visited[name] = true

	node, ok := g[name]
	if !ok {
		return
	}

	for _, neighbor := range node.Children {
		if !visited[neighbor.Name] {
			g.dfs(neighbor.Name, visited)
		}
	}
}

func (g Graph) LeafNodes() []Node {
	var leafNodes []Node
	for _, node := range g {
		if len(node.Children) == 0 {
			leafNodes = append(leafNodes, *node)
		}
	}
	return leafNodes
}
