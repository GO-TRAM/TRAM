// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	emptySpecName         = "empty stream spec"
	specWithSource        = "stream spec with source"
	specWithSourceAndSink = "stream spec with source and sink"
)

func TestTreeString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		streamSpec  domain.StreamSpec
		expected    string
		expectedErr error
	}{
		{
			name:       "empty stream tree",
			streamSpec: domain.StreamSpec{},
			expected:   "source:->[]",
		},
		{
			name: "single node",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
			},
			expected: "source:testSource->[]",
		},
		{
			name: "single node with child",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: "source:testSource->[sink:testSink->[]]",
		},
		{
			name: "multiple nodes with children",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage",
					},
					{
						Name: "testSink2",
						From: "testSource",
					},
				},
			},
			expected: "source:testSource->[stage:testStage->[sink:testSink->[]],sink:testSink2->[]]",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			tree, err := domain.NewTree(c.streamSpec)
			assert.Equal(t, c.expectedErr, err)
			actual := tree.String()
			if actual != c.expected {
				t.Errorf("Expected %s, got %s", c.expected, actual)
			}
		})
	}
}

func TestNodeString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		node     domain.Node
		expected string
	}{
		{
			name:     "empty node",
			node:     domain.Node{},
			expected: ":->[]",
		},
		{
			name: "node with name and kind",
			node: domain.Node{
				Name: "testSource",
				Kind: "source",
			},
			expected: "source:testSource->[]",
		},
		{
			name: "node with name, kind, and children",
			node: domain.Node{
				Name: "testSource",
				Kind: "source",
				Children: []*domain.Node{
					&domain.Node{
						Name: "testSink",
						Kind: "sink",
					},
				},
			},
			expected: "source:testSource->[sink:testSink->[]]",
		},
		{
			name: "node with name, kind, and nested children",
			node: domain.Node{
				Name: "testSource",
				Kind: "source",
				Children: []*domain.Node{
					&domain.Node{
						Name: "testSink",
						Kind: "sink",
					},
					&domain.Node{
						Name: "testStage",
						Kind: "stage",
						Children: []*domain.Node{
							&domain.Node{
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
			node: domain.Node{
				Name: "testSource",
				Kind: "source",
				Children: []*domain.Node{
					&domain.Node{
						Name: "testStage",
						Kind: "stage",
						Children: []*domain.Node{
							&domain.Node{
								Name: "testSink2",
								Kind: "sink",
							},
						},
					},
					&domain.Node{
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
			t.Parallel()

			actual := c.node.String()
			if actual != c.expected {
				t.Errorf("Expected %s, got %s", c.expected, actual)
			}
		})
	}
}

func TestNewTree(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		streamSpec  domain.StreamSpec
		expectedErr error
	}{
		{
			name:       emptySpecName,
			streamSpec: domain.StreamSpec{},
		},
		{
			name: specWithSource,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
			},
			expectedErr: nil,
		},
		{
			name: specWithSourceAndSink,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "stream spec with duplicate stages",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
					{
						Name: "testStage",
						From: "testSource",
					},
				},
			},
			expectedErr: domain.ErrStageDuplicate,
		},
		{
			name: "stream spec with invalid stage from",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
					{
						Name: "testStage2",
						From: "testStage3",
					},
				},
			},
			expectedErr: domain.ErrStageInvalidFrom,
		},
		{
			name: "stream spec with duplicate sinks",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expectedErr: domain.ErrSinkDuplicate,
		},
		{
			name: "stream spec with invalid sink from",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
					{
						Name: "testSink2",
						From: "testSink3",
					},
				},
			},
			expectedErr: domain.ErrSinkInvalidFrom,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewTree(c.streamSpec)
			assert.Equal(t, c.expectedErr, err)
		})
	}
}

func TestLeafNodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		streamSpec domain.StreamSpec
		expected   []string
	}{
		{
			name: "empty stream tree",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "source",
				},
			},
			expected: []string{"source"},
		},
		{
			name: "single node with child",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: []string{"testSink"},
		},
		{
			name: "multiple nodes with children",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage",
					},
					{
						Name: "testSink2",
						From: "testSource",
					},
				},
			},
			expected: []string{"testSink", "testSink2"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			tree, err := domain.NewTree(c.streamSpec)
			require.NoError(t, err)
			var actual []string
			for _, node := range tree.LeafNodes() {
				actual = append(actual, node.Name)
			}
			assert.ElementsMatch(t, c.expected, actual)
		})
	}
}

func TestIsTree(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		streamSpec domain.StreamSpec
		expected   bool
	}{
		{
			name:       emptySpecName,
			streamSpec: domain.StreamSpec{},
			expected:   true,
		},
		{
			name: specWithSource,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
			},
			expected: true,
		},
		{
			name: specWithSourceAndSink,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source and stage",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, stage, and sink",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, stage, and sink from source",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, stage, and sink from another stage",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage1",
						From: "testSource",
					},
					{
						Name: "testStage2",
						From: "testStage1",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage2",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, multiple stages, and sinks",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage1",
						From: "testSource",
					},
					{
						Name: "testStage2",
						From: "testStage1",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink1",
						From: "testStage1",
					},
					{
						Name: "testSink2",
						From: "testStage2",
					},
				},
			},
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			tree, err := domain.NewTree(c.streamSpec)
			require.NoError(t, err)
			actual := tree.IsTree()
			if actual != c.expected {
				t.Errorf("Expected %t, got %t", c.expected, actual)
			}
		})
	}
}

func TestIsConnected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		streamSpec domain.StreamSpec
		expected   bool
	}{
		{
			name: specWithSourceAndSink,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source and stage",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source and circular stages",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage1",
						From: "testStage2",
					},
					{
						Name: "testStage2",
						From: "testStage1",
					},
				},
			},
			expected: false,
		},
		{
			name: "stream spec with source, stage, and sink",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, stage, and sink from source",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage",
						From: "testSource",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, stage, and sink from another stage",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage1",
						From: "testSource",
					},
					{
						Name: "testStage2",
						From: "testStage1",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testStage2",
					},
				},
			},
			expected: true,
		},
		{
			name: "stream spec with source, multiple stages, and sinks",
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Stages: []domain.StageSpec{
					{
						Name: "testStage1",
						From: "testSource",
					},
					{
						Name: "testStage2",
						From: "testStage1",
					},
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink1",
						From: "testStage1",
					},
					{
						Name: "testSink2",
						From: "testStage2",
					},
				},
			},
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			tree, err := domain.NewTree(c.streamSpec)
			require.NoError(t, err)
			actual := tree.IsConnected()
			if actual != c.expected {
				t.Errorf("Expected %t, got %t", c.expected, actual)
			}
		})
	}
}

func BenchmarkNewTree(b *testing.B) {
	cases := []struct {
		name       string
		streamSpec domain.StreamSpec
	}{
		{
			name:       emptySpecName,
			streamSpec: domain.StreamSpec{},
		},
		{
			name: specWithSource,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
			},
		},
		{
			name: specWithSourceAndSink,
			streamSpec: domain.StreamSpec{
				Source: domain.SourceSpec{
					Name: "testSource",
				},
				Sinks: []domain.SinkSpec{
					{
						Name: "testSink",
						From: "testSource",
					},
				},
			},
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for range b.N {
				_, _ = domain.NewTree(c.streamSpec)
			}
		})
	}
}
