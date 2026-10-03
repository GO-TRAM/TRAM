package core

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

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
func (c *Core) buildStream(tree *domain.Tree) error {
	err := c.createAll(tree)
	if err == nil {
		err = c.wire(tree, true)
	}
	if err != nil {
		c.stopUnbuilt(tree, true)
	}
	return err
}

func (c *Core) createAll(tree *domain.Tree) error {
	spec := tree.Spec
	source, err := c.sourceFactory.New(spec.Source)
	if err != nil {
		return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}
	if err = tree.Root().AddSource(source); err != nil {
		return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, spec.Source.Name), err)
	}
	return c.createReceivers(tree)
}

// buildReceivers creates and wires the stages and sinks of tree below a
// source that is already set on its root. The source is not subscribed to its
// children; see replaceReceivers. On failure, the stages and sinks created so
// far are stopped again, the source is left untouched.
func (c *Core) buildReceivers(tree *domain.Tree) error {
	err := c.createReceivers(tree)
	if err == nil {
		err = c.wire(tree, false)
	}
	if err != nil {
		c.stopUnbuilt(tree, false)
	}
	return err
}

// stopUnbuilt stops the components created for a stream that could not be
// built.
func (c *Core) stopUnbuilt(tree *domain.Tree, includeSource bool) {
	if err := stopNodes(tree, includeSource); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not stop components of unbuilt stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}
}

// createReceivers creates the stages and sinks of tree.
func (c *Core) createReceivers(tree *domain.Tree) error {
	for _, stageSpec := range tree.Spec.Stages {
		stage, err := c.stageFactory.New(stageSpec)
		if err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
		if err = tree.GetNode(stageSpec.Name).AddStage(stage); err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, stageSpec.Name), err)
		}
	}

	for _, sinkSpec := range tree.Spec.Sinks {
		sink, err := c.sinkFactory.New(sinkSpec)
		if err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
		if err = tree.GetNode(sinkSpec.Name).AddSink(sink); err != nil {
			return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, sinkSpec.Name), err)
		}
	}
	return nil
}

// wire subscribes every node to its parent. The root's children are not
// subscribed to the source themselves; with includeRoot set, the source gets
// a fan-out to them, see sourceSubscriber.
func (c *Core) wire(tree *domain.Tree, includeRoot bool) error {
	root := tree.Root()
	for _, node := range tree.Map() {
		if node.Parent == nil || node.Parent == root {
			continue
		}
		observer, err := node.Parent.GetObserver()
		if err != nil {
			return err
		}
		subscriber, err := c.subscriber(tree, node)
		if err != nil {
			return err
		}
		if err = observer.Subscribe(subscriber); err != nil {
			return err
		}
	}
	if !includeRoot {
		return nil
	}

	source, err := root.GetObserver()
	if err != nil {
		return err
	}
	subscriber, err := c.sourceSubscriber(tree)
	if err != nil {
		return err
	}
	return source.Subscribe(subscriber)
}

// subscriber returns the subscriber of node, wrapped so that it is measured.
// Every subscriber the core connects has to come from here.
func (c *Core) subscriber(tree *domain.Tree, node *domain.Node) (domain.Subscriber, error) {
	subscriber, err := node.GetSubscriber()
	if err != nil {
		return nil, err
	}
	return c.metrics.instrument(tree.Spec.Name, node, subscriber), nil
}

// sourceSubscriber returns the subscriber the source of tree delivers to: a
// fan-out to the source's children, measured as the source. Every source
// gets its subscriber from here.
func (c *Core) sourceSubscriber(tree *domain.Tree) (domain.Subscriber, error) {
	root := tree.Root()
	children := make([]domain.Subscriber, 0, len(root.Children))
	for _, child := range root.Children {
		subscriber, err := c.subscriber(tree, child)
		if err != nil {
			return nil, err
		}
		children = append(children, subscriber)
	}
	return c.metrics.instrument(tree.Spec.Name, root, &fanout{name: root.Name, children: children}), nil
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
	if err = tree.Root().AddSource(source); err != nil {
		return err
	}
	if err = c.buildReceivers(tree); err != nil {
		return err
	}

	subscriber, err := c.sourceSubscriber(tree)
	if err != nil {
		return err
	}

	// Waits for events still being delivered to the old stages and sinks, so
	// they can be stopped right after.
	source.SetSubscribers([]domain.Subscriber{subscriber})
	if err = stopNodes(old.tree, false); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not cleanly stop replaced components of stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}

	c.streams[tree.Spec.Name] = &stream{tree: tree, done: old.done}
	return nil
}

// runStream starts the source of a built tree and registers the stream.
// restarts counts the restarts in a row that led to this run. Must be called
// with the write lock held.
func (c *Core) runStream(tree *domain.Tree, restarts int) {
	s := &stream{
		tree: tree,
		done: make(chan struct{}),
	}
	c.streams[tree.Spec.Name] = s

	c.wg.Go(func() {
		started := time.Now()
		c.runSource(tree)
		close(s.done)

		if time.Since(started) >= c.maxRestartDelay {
			restarts = 0
		}
		c.scheduleRestart(tree.Spec.Name, s.done, restarts)
	})
}

// runSource runs the source of tree until it returns.
func (c *Core) runSource(tree *domain.Tree) {
	c.logger.InfoContext(c.ctx, "Running stream", slog.String("stream", tree.Spec.Name))
	source, err := tree.Root().GetSource()
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not get source of stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
		return
	}
	if err = source.Run(c.ctx); err != nil {
		c.logger.ErrorContext(c.ctx, "Stream stopped with an error",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}
}

// scheduleRestart restarts the stream whose source run done belongs to, after
// a delay that grows with restarts. Sources also return when they are
// stopped, but their stream is removed or replaced by then, which
// restartStream detects.
func (c *Core) scheduleRestart(name string, done chan struct{}, restarts int) {
	if c.ctx.Err() != nil {
		return
	}
	time.AfterFunc(c.restartDelay(restarts), func() {
		c.restartStream(name, done, restarts+1)
	})
}

func (c *Core) restartDelay(restarts int) time.Duration {
	delay := c.minRestartDelay
	for range restarts {
		delay *= 2
		if delay >= c.maxRestartDelay {
			return c.maxRestartDelay
		}
	}
	return delay
}

// restartStream rebuilds a stream whose source returned on its own, unless
// the stream was removed or replaced meanwhile. If rebuilding fails, the
// stream stays registered with its components stopped and is tried again
// later.
func (c *Core) restartStream(name string, done chan struct{}, restarts int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	old, exists := c.streams[name]
	if c.closed || !exists || old.done != done {
		return
	}

	c.logger.WarnContext(c.ctx, "Restarting stream whose source stopped",
		slog.String("stream", name), slog.Int("restart", restarts))
	if err := c.stopStream(old); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not cleanly stop stream",
			slog.String("stream", name), slog.Any("error", err))
	}

	tree, err := newTree(old.tree.Spec)
	if err == nil {
		err = c.buildStream(tree)
	}
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not restart stream",
			slog.String("stream", name), slog.Any("error", err))
		c.scheduleRestart(name, done, restarts)
		return
	}
	c.runStream(tree, restarts)
}

// restoreStream rebuilds and starts a previously running stream after a
// failed update stopped it. If that fails too, the stopped stream stays
// registered and is restarted like a stream whose source returned on its own:
// stopping its source scheduled that restart already. Must be called with the
// write lock held.
func (c *Core) restoreStream(spec domain.StreamSpec) {
	tree, err := newTree(spec)
	if err == nil {
		err = c.buildStream(tree)
	}
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not restore stream after failed update, retrying later",
			slog.String("stream", spec.Name), slog.Any("error", err))
		return
	}
	c.runStream(tree, 0)
}

// stopStream stops the source, waits for it to return so no event is in
// flight, and then stops all stages and sinks. A stream is stopped only once;
// later calls do nothing.
func (c *Core) stopStream(s *stream) error {
	if s.stopped {
		return nil
	}
	s.stopped = true

	source, err := s.tree.Root().GetSource()
	if err != nil {
		return errors.Join(domain.ErrStreamStopFailed, err)
	}

	var errs []error
	if err = source.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", source.Name(), err))
	}
	<-s.done

	if err = stopNodes(s.tree, false); err != nil {
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
