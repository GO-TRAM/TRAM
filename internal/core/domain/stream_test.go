package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	cases := []struct {
		name        string
		stream      StreamSpec
		expected    Tree
		expectedErr error
	}{
		{
			name: "simple stream",
			stream: StreamSpec{
				Name: "testStream",
				Source: SourceSpec{
					Name: "testSource",
					Type: "test",
				},
				Sinks: []SinkSpec{
					{
						Name: "testSink",
						Type: "test",
						From: "testSource",
					},
				},
			},
			expected: Tree{
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
		},
		{
			name: "fanout stream",
			stream: StreamSpec{
				Source: SourceSpec{
					Name: "testSource",
					Type: "test",
				},
				Stages: []StageSpec{
					{
						Name: "testStage",
						Type: "test",
						From: "testSource",
					},
				},
				Sinks: []SinkSpec{
					{
						Name: "testSink",
						Type: "test",
						From: "testSource",
					},
					{
						Name: "testSink2",
						Type: "test",
						From: "testStage",
					},
				},
			},
			expected: Tree{
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
		},
		{
			name: "double stage",
			stream: StreamSpec{
				Source: SourceSpec{
					Name: "testSource",
					Type: "test",
				},
				Stages: []StageSpec{
					{
						Name: "testStage",
						Type: "test",
						From: "testSource",
					},
					{
						Name: "testStage2",
						Type: "test",
						From: "testStage",
					},
				},
				Sinks: []SinkSpec{
					{
						Name: "testSink",
						Type: "test",
						From: "testSource",
					},
					{
						Name: "testSink2",
						Type: "test",
						From: "testStage2",
					},
				},
			},
			expected: Tree{
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
									Name: "testStage2",
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
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree, err := c.stream.Tree()
			assert.Equal(t, c.expectedErr, err)
			assert.Equal(t, c.expected.String(), tree.String())
		})
	}
}
