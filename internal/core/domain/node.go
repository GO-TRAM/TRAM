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

const (
	orderSource = iota
	orderStage
	orderSink
)

const orderUnknown = -1

func (k NodeKind) Value() int {
	switch k {
	case KindSource:
		return orderSource
	case KindStage:
		return orderStage
	case KindSink:
		return orderSink
	default:
		return orderUnknown
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
	sink, ok := n.Processor.(Sink)
	if n.Kind != KindSink || !ok {
		return nil, ErrNodeNotSink
	}
	return sink, nil
}

func (n *Node) AddStage(s Stage) error {
	if n.Kind != KindStage {
		return ErrNodeNotStage
	}
	n.Processor = s
	return nil
}

func (n *Node) GetStage() (Stage, error) {
	stage, ok := n.Processor.(Stage)
	if n.Kind != KindStage || !ok {
		return nil, ErrNodeNotStage
	}
	return stage, nil
}

func (n *Node) AddSource(s Source) error {
	if n.Kind != KindSource {
		return ErrNodeNotSource
	}
	n.Processor = s
	return nil
}

func (n *Node) GetSource() (Source, error) {
	source, ok := n.Processor.(Source)
	if n.Kind != KindSource || !ok {
		return nil, ErrNodeNotSource
	}
	return source, nil
}

func (n *Node) GetObserver() (Observer, error) {
	observer, ok := n.Processor.(Observer)
	if (n.Kind != KindSource && n.Kind != KindStage) || !ok {
		return nil, ErrNodeNotObserver
	}
	return observer, nil
}

func (n *Node) GetSubscriber() (Subscriber, error) {
	subscriber, ok := n.Processor.(Subscriber)
	if (n.Kind != KindStage && n.Kind != KindSink) || !ok {
		return nil, ErrNodeNotSubscriber
	}
	return subscriber, nil
}

func (n *Node) String() string {
	var children []Node
	for _, child := range n.Children {
		children = append(children, *child)
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
