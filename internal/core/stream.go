package core

import (
	"errors"
	"fmt"
	"log"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

// newTree builds the node structure of a stream and validates its shape
// without creating any components.
func newTree(spec domain.StreamSpec) (*domain.Tree, error) {
	tree, err := domain.NewTree(spec)
	if err != nil {
		return nil, err
	}

	if !tree.IsTree() {
		return nil, domain.ErrGraphNotTree
	}

	if !tree.IsConnected() {
		return nil, domain.ErrGraphNotConnected
	}

	for _, node := range tree.LeafNodes() {
		if node.Kind != domain.KindSink {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphLeafNotSink)
		}
	}

	for _, node := range tree.Map() {
		if node.Kind == domain.KindSink && len(node.Children) != 0 {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphSinkHasChildren)
		}
	}

	return tree, nil
}

// buildStream creates and wires all components of tree. On failure, the
// components created so far are stopped again.
func (c *Core) buildStream(tree *domain.Tree) (err error) {
	defer func() {
		if err != nil {
			if stopErr := stopNodes(tree, true); stopErr != nil {
				log.Printf("failed to stop components of unbuilt stream %s: %v", tree.Spec.Name, stopErr)
			}
		}
	}()

	spec := tree.Spec
	source, err := c.sourceFactory.New(spec.Source)
	if err != nil {
		return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}
	if err := tree.Root().AddSource(source); err != nil {
		return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}

	if err := c.createReceivers(tree); err != nil {
		return err
	}
	return wire(tree, true)
}

// buildReceivers creates and wires the stages and sinks of tree below a
// source that is already set on its root. The source is not subscribed to its
// children; see replaceReceivers. On failure, the stages and sinks created so
// far are stopped again, the source is left untouched.
func (c *Core) buildReceivers(tree *domain.Tree) (err error) {
	defer func() {
		if err != nil {
			if stopErr := stopNodes(tree, false); stopErr != nil {
				log.Printf("failed to stop components of unbuilt stream %s: %v", tree.Spec.Name, stopErr)
			}
		}
	}()

	if err := c.createReceivers(tree); err != nil {
		return err
	}
	return wire(tree, false)
}

// createReceivers creates the stages and sinks of tree.
func (c *Core) createReceivers(tree *domain.Tree) error {
	for _, stageSpec := range tree.Spec.Stages {
		stage, err := c.stageFactory.New(stageSpec)
		if err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
		if err := tree.GetNode(stageSpec.Name).AddStage(stage); err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
	}

	for _, sinkSpec := range tree.Spec.Sinks {
		sink, err := c.sinkFactory.New(sinkSpec)
		if err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
		if err := tree.GetNode(sinkSpec.Name).AddSink(sink); err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
	}
	return nil
}

// wire subscribes every node to its parent. With includeRoot unset, the
// root's children are left out.
func wire(tree *domain.Tree, includeRoot bool) error {
	root := tree.Root()
	for _, node := range tree.Map() {
		if node.Parent == nil || (node.Parent == root && !includeRoot) {
			continue
		}
		observer, err := node.Parent.GetObserver()
		if err != nil {
			return err
		}
		subscriber, err := node.GetSubscriber()
		if err != nil {
			return err
		}
		if err := observer.Subscribe(subscriber); err != nil {
			return err
		}
	}
	return nil
}

// replaceReceivers updates a running stream whose source is unchanged: it
// builds the new stages and sinks, switches the running source over to them,
// and stops the old ones. The source keeps running, so a source like OTLP
// keeps its port open. If building fails, the running stream is not touched.
// Must be called with the write lock held.
func (c *Core) replaceReceivers(old *stream, tree *domain.Tree) error {
	source, err := old.tree.Root().GetSource()
	if err != nil {
		return err
	}
	if err := tree.Root().AddSource(source); err != nil {
		return err
	}
	if err := c.buildReceivers(tree); err != nil {
		return err
	}

	children := make([]domain.Subscriber, 0, len(tree.Root().Children))
	for _, child := range tree.Root().Children {
		subscriber, err := child.GetSubscriber()
		if err != nil {
			return err
		}
		children = append(children, subscriber)
	}

	// Waits for events still being delivered to the old stages and sinks, so
	// they can be stopped right after.
	source.SetSubscribers(children)
	if err := stopNodes(old.tree, false); err != nil {
		log.Printf("failed to cleanly stop replaced components of stream %s: %v", tree.Spec.Name, err)
	}

	c.streams[tree.Spec.Name] = &stream{tree: tree, done: old.done}
	return nil
}

// runStream starts the source of a built tree and registers the stream.
// Must be called with the write lock held.
func (c *Core) runStream(tree *domain.Tree) {
	s := &stream{
		tree: tree,
		done: make(chan struct{}),
	}
	c.streams[tree.Spec.Name] = s

	c.wg.Go(func() {
		defer close(s.done)
		log.Println("Running stream", tree.Spec.Name)
		source, err := tree.Root().GetSource()
		if err != nil {
			log.Println("Failed to get source for stream", tree.Spec.Name, err)
			return
		}
		if err := source.Run(c.ctx); err != nil {
			log.Println("Failed to run stream", tree.Spec.Name, err)
			return
		}
	})
}

// restoreStream rebuilds and starts a previously running stream after a
// failed update replaced it. Must be called with the write lock held.
func (c *Core) restoreStream(spec domain.StreamSpec) {
	tree, err := newTree(spec)
	if err == nil {
		err = c.buildStream(tree)
	}
	if err != nil {
		log.Printf("failed to restore stream %s after failed update: %v", spec.Name, err)
		return
	}
	c.runStream(tree)
}

// stopStream stops the source, waits for it to return so no event is in
// flight, and then stops all stages and sinks.
func (c *Core) stopStream(s *stream) error {
	source, err := s.tree.Root().GetSource()
	if err != nil {
		return errors.Join(domain.ErrStreamStopFailed, err)
	}

	var errs []error
	if err := source.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", source.Name(), err))
	}
	<-s.done

	if err := stopNodes(s.tree, false); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(append([]error{domain.ErrStreamStopFailed}, errs...)...)
	}
	return nil
}

// stopNodes stops every created stage and sink of tree, and the source if
// includeSource is set. Nodes are stopped top-down, so a stage that still
// holds events (like a batch) can flush them to children that are running.
func stopNodes(tree *domain.Tree, includeSource bool) error {
	var errs []error
	queue := []*domain.Node{tree.Root()}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)

		if node.Kind == domain.KindSource && !includeSource {
			continue
		}
		stoppable, ok := node.Processor.(domain.Stoppable)
		if !ok {
			continue
		}
		if err := stoppable.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", node.Name, err))
		}
	}
	return errors.Join(errs...)
}
