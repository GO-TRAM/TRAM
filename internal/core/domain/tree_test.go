package domain

import "testing"

func TestTreeString(t *testing.T) {
	cases := []struct {
		name     string
		tree     Tree
		expected string
	}{
		{
			name:     "empty tree",
			tree:     Tree{},
			expected: ":->[]",
		},
		{
			name: "single node",
			tree: Tree{
				"a": &Node{
					Name: "testSource",
					Kind: "source",
				},
			},
			expected: "source:testSource->[]",
		},
		{
			name: "single node with child",
			tree: Tree{
				"a": &Node{
					Name: "testSource",
					Kind: "source",
					Children: []*Node{
						&Node{
							Name: "testSink",
							Kind: "sink",
						},
					},
				},
			},
			expected: "source:testSource->[sink:testSink->[]]",
		},
		{
			name: "multiple nodes with children",
			tree: Tree{
				"a": &Node{
					Name: "testSource",
					Kind: "source",
					Children: []*Node{
						&Node{
							Name: "testSink",
							Kind: "sink",
						},
						&Node{
							Name: "testStage",
							Kind: "stage",
							Children: []*Node{
								&Node{
									Name: "testSink2",
									Kind: "sink",
								},
							},
						},
					},
				},
			},
			expected: "source:testSource->[stage:testStage->[sink:testSink2->[]],sink:testSink->[]]",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := c.tree.String()
			if actual != c.expected {
				t.Errorf("Expected %s, got %s", c.expected, actual)
			}
		})
	}
}

func TestNodeString(t *testing.T) {
	cases := []struct {
		name     string
		node     Node
		expected string
	}{
		{
			name:     "empty node",
			node:     Node{},
			expected: ":->[]",
		},
		{
			name: "node with name and kind",
			node: Node{
				Name: "testSource",
				Kind: "source",
			},
			expected: "source:testSource->[]",
		},
		{
			name: "node with name, kind, and children",
			node: Node{
				Name: "testSource",
				Kind: "source",
				Children: []*Node{
					&Node{
						Name: "testSink",
						Kind: "sink",
					},
				},
			},
			expected: "source:testSource->[sink:testSink->[]]",
		},
		{
			name: "node with name, kind, and nested children",
			node: Node{
				Name: "testSource",
				Kind: "source",
				Children: []*Node{
					&Node{
						Name: "testSink",
						Kind: "sink",
					},
					&Node{
						Name: "testStage",
						Kind: "stage",
						Children: []*Node{
							&Node{
								Name: "testSink2",
								Kind: "sink",
							},
						},
					},
				},
			},
			expected: "source:testSource->[stage:testStage->[sink:testSink2->[]],sink:testSink->[]]",
		},
		{
			name: "node with name, kind, and nested children in different order",
			node: Node{
				Name: "testSource",
				Kind: "source",
				Children: []*Node{
					&Node{
						Name: "testStage",
						Kind: "stage",
						Children: []*Node{
							&Node{
								Name: "testSink2",
								Kind: "sink",
							},
						},
					},
					&Node{
						Name: "testSink",
						Kind: "sink",
					},
				},
			},
			expected: "source:testSource->[stage:testStage->[sink:testSink2->[]],sink:testSink->[]]",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := c.node.String()
			if actual != c.expected {
				t.Errorf("Expected %s, got %s", c.expected, actual)
			}
		})
	}
}
