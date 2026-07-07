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

type Node struct {
	Name      string
	Kind      NodeKind
	Parent    *Node
	Children  []*Node
	Processor any
}

func (n *Node) AddSink(s Sink) error {
	if n.Kind != KindSink {
		return ErrNodeNotSink
	}
	n.Processor = s
	return nil
}

func (n *Node) GetSink() (Sink, error) {
	if n.Kind != KindSink {
		return nil, ErrNodeNotSink
	}
	return n.Processor.(Sink), nil
}

func (n *Node) AddStage(s Stage) error {
	if n.Kind != KindStage {
		return ErrNodeNotStage
	}
	n.Processor = s
	return nil
}

func (n *Node) GetStage() (Stage, error) {
	if n.Kind != KindStage {
		return nil, ErrNodeNotStage
	}
	return n.Processor.(Stage), nil
}

func (n *Node) AddSource(s Source) error {
	if n.Kind != KindSource {
		return ErrNodeNotSource
	}
	n.Processor = s
	return nil
}

func (n *Node) GetSource() (Source, error) {
	if n.Kind != KindSource {
		return nil, ErrNodeNotSource
	}
	return n.Processor.(Source), nil
}

func (n *Node) GetObserver() (Observer, error) {
	if n.Kind != KindSource && n.Kind != KindStage {
		return nil, ErrNodeNotObserver
	}
	return n.Processor.(Observer), nil
}

func (n Node) GetSubscriber() (Subscriber, error) {
	if n.Kind != KindStage && n.Kind != KindSink {
		return nil, ErrNodeNotSubscriber
	}
	return n.Processor.(Subscriber), nil
}

func (n *Node) String() string {
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
