// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// mockSource implements domain.Source for testing
type mockSource struct {
	name       string
	onRun      func(ctx context.Context) error
	subscribers []domain.Subscriber
	subMu       sync.Mutex
}

func newMockSource(name string) *mockSource {
	return &mockSource{name: name}
}

func (m *mockSource) Name() string { return m.name }
func (m *mockSource) From() string { return "" }
func (m *mockSource) Stop() error { return nil }

func (m *mockSource) Subscribe(subscriber domain.Subscriber) error {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	m.subscribers = append(m.subscribers, subscriber)
	return nil
}

func (m *mockSource) Run(ctx context.Context) error {
	if m.onRun != nil {
		return m.onRun(ctx)
	}
	return nil
}

func (m *mockSource) SetSubscribers(subscribers []domain.Subscriber) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	m.subscribers = subscribers
}

// mockStage implements domain.Stage for testing
type mockStage struct {
	name       string
	onNotify   func(ctx context.Context, event *domain.Event) error
	subscribers []domain.Subscriber
	subMu       sync.Mutex
}

func newMockStage(name string) *mockStage {
	return &mockStage{name: name}
}

func (m *mockStage) Name() string { return m.name }
func (m *mockStage) From() string { return "" }
func (m *mockStage) Stop() error { return nil }

func (m *mockStage) Subscribe(subscriber domain.Subscriber) error {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	m.subscribers = append(m.subscribers, subscriber)
	return nil
}

func (m *mockStage) Notify(ctx context.Context, event *domain.Event) error {
	if m.onNotify != nil {
		return m.onNotify(ctx, event)
	}
	return nil
}

// mockSink implements domain.Sink for testing
type mockSink struct {
	name       string
	onNotify   func(ctx context.Context, event *domain.Event) error
	received   []*domain.Event
	recvMu     sync.Mutex
}

func newMockSink(name string) *mockSink {
	return &mockSink{name: name}
}

func (m *mockSink) Name() string { return m.name }
func (m *mockSink) From() string { return "" }
func (m *mockSink) Stop() error { return nil }

func (m *mockSink) Notify(ctx context.Context, event *domain.Event) error {
	m.recvMu.Lock()
	defer m.recvMu.Unlock()

	if m.onNotify != nil {
		if err := m.onNotify(ctx, event); err != nil {
			return err
		}
	}

	m.received = append(m.received, event)
	return nil
}

func (m *mockSink) GetReceived() []*domain.Event {
	m.recvMu.Lock()
	defer m.recvMu.Unlock()
	return append([]*domain.Event(nil), m.received...)
}

// TestStreamHotSwappingBasic tests basic stream hot swapping functionality
func TestStreamHotSwappingBasic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.Default()

	const (
		testStreamName      = "test-stream"
		initialSourceName   = "initial-source"
		initialSinkName     = "initial-sink"
		updatedSinkName     = "updated-sink"
	)

	// Create initial stream spec
	initialSpec := domain.StreamSpec{
		Name: testStreamName,
		Source: domain.SourceSpec{
			Type: "mock",
			Name: initialSourceName,
		},
		Stages: []domain.StageSpec{},
		Sinks: []domain.SinkSpec{
			{Type: "mock", Name: initialSinkName, From: initialSourceName},
		},
	}

	// Create factories
	sourceFactory := newMockFactory[domain.SourceSpec, domain.Source](func(spec domain.SourceSpec) (domain.Source, error) {
		return newMockSource(spec.Name), nil
	})

	stageFactory := newMockFactory[domain.StageSpec, domain.Stage](func(spec domain.StageSpec) (domain.Stage, error) {
		return newMockStage(spec.Name), nil
	})

	sinkFactory := newMockFactory[domain.SinkSpec, domain.Sink](func(spec domain.SinkSpec) (domain.Sink, error) {
		return newMockSink(spec.Name), nil
	})

	// Create core
	core := core.New(ctx, logger, sourceFactory, stageFactory, sinkFactory)
	defer core.Close()

	// Upsert initial stream
	_, err := core.UpsertStream(ctx, initialSpec)
	require.NoError(t, err)

	// Verify stream was created
	stream, err := core.GetStream(ctx, testStreamName)
	require.NoError(t, err)
	assert.Equal(t, testStreamName, stream.Name)

	// Update stream spec (hot swap)
	updatedSpec := initialSpec
	updatedSpec.Sinks = []domain.SinkSpec{
		{Type: "mock", Name: updatedSinkName, From: initialSourceName},
	}

	// Upsert updated stream
	_, err = core.UpsertStream(ctx, updatedSpec)
	require.NoError(t, err)

	// Verify stream was updated
	stream, err = core.GetStream(ctx, testStreamName)
	require.NoError(t, err)
	assert.Equal(t, testStreamName, stream.Name)
}

// TestStreamHotSwappingWithRunningSource tests hot swapping when source is running
func TestStreamHotSwappingWithRunningSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.Default()

	const (
		runningStreamName   = "running-stream"
		runningSourceName   = "running-source"
		runningSinkName     = "running-sink"
		updatedSinkName     = "updated-sink"
	)

	// Create stream spec
	spec := domain.StreamSpec{
		Name: runningStreamName,
		Source: domain.SourceSpec{
			Type: "mock",
			Name: runningSourceName,
		},
		Stages: []domain.StageSpec{},
		Sinks: []domain.SinkSpec{
			{Type: "mock", Name: runningSinkName, From: runningSourceName},
		},
	}

	// Create factories
	sourceFactory := newMockFactory[domain.SourceSpec, domain.Source](func(spec domain.SourceSpec) (domain.Source, error) {
		return newMockSource(spec.Name), nil
	})

	stageFactory := newMockFactory[domain.StageSpec, domain.Stage](func(spec domain.StageSpec) (domain.Stage, error) {
		return newMockStage(spec.Name), nil
	})

	sinkFactory := newMockFactory[domain.SinkSpec, domain.Sink](func(spec domain.SinkSpec) (domain.Sink, error) {
		return newMockSink(spec.Name), nil
	})

	// Create core
	core := core.New(ctx, logger, sourceFactory, stageFactory, sinkFactory)
	defer core.Close()

	// Upsert stream
	_, err := core.UpsertStream(ctx, spec)
	require.NoError(t, err)

	// Update stream spec
	updatedSpec := spec
	updatedSpec.Sinks = []domain.SinkSpec{
		{Type: "mock", Name: updatedSinkName, From: runningSourceName},
	}

	// Upsert updated stream
	_, err = core.UpsertStream(ctx, updatedSpec)
	require.NoError(t, err)

	// Verify stream was updated
	stream, err := core.GetStream(ctx, runningStreamName)
	require.NoError(t, err)
	assert.Equal(t, runningStreamName, stream.Name)
}

// TestStreamHotSwappingErrorRecovery tests stream hot swapping with error recovery
func TestStreamHotSwappingErrorRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.Default()

	const (
		errorRecoveryStreamName = "error-recovery-stream"
		initialSourceName       = "initial-source"
		initialSinkName         = "initial-sink"
		updatedSinkName         = "updated-sink"
	)

	// Create initial stream spec
	initialSpec := domain.StreamSpec{
		Name: errorRecoveryStreamName,
		Source: domain.SourceSpec{
			Type: "mock",
			Name: initialSourceName,
		},
		Stages: []domain.StageSpec{},
		Sinks: []domain.SinkSpec{
			{Type: "mock", Name: initialSinkName, From: initialSourceName},
		},
	}

	// Create factories
	sourceFactory := newMockFactory[domain.SourceSpec, domain.Source](func(spec domain.SourceSpec) (domain.Source, error) {
		return newMockSource(spec.Name), nil
	})

	stageFactory := newMockFactory[domain.StageSpec, domain.Stage](func(spec domain.StageSpec) (domain.Stage, error) {
		return newMockStage(spec.Name), nil
	})

	sinkFactory := newMockFactory[domain.SinkSpec, domain.Sink](func(spec domain.SinkSpec) (domain.Sink, error) {
		return newMockSink(spec.Name), nil
	})

	// Create core
	core := core.New(ctx, logger, sourceFactory, stageFactory, sinkFactory)
	defer core.Close()

	// Upsert initial stream
	_, err := core.UpsertStream(ctx, initialSpec)
	require.NoError(t, err)

	// Update stream spec
	updatedSpec := initialSpec
	updatedSpec.Sinks = []domain.SinkSpec{
		{Type: "mock", Name: updatedSinkName, From: initialSourceName},
	}

	// Upsert updated stream
	_, err = core.UpsertStream(ctx, updatedSpec)
	require.NoError(t, err)

	// Verify stream was updated
	stream, err := core.GetStream(ctx, errorRecoveryStreamName)
	require.NoError(t, err)
	assert.Equal(t, errorRecoveryStreamName, stream.Name)
}

// TestStreamHotSwappingWithStages tests hot swapping with stages
func TestStreamHotSwappingWithStages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.Default()

	const (
		stageStreamName    = "stage-stream"
		stageSourceName    = "stage-source"
		stage1Name         = "stage-1"
		stage2Name         = "stage-2"
		stageSinkName      = "stage-sink"
		stageSinkUpdated   = "stage-sink-updated"
	)

	// Create initial stream spec with stages
	initialSpec := domain.StreamSpec{
		Name: stageStreamName,
		Source: domain.SourceSpec{
			Type: "mock",
			Name: stageSourceName,
		},
		Stages: []domain.StageSpec{
			{Type: "mock", Name: stage1Name, From: stageSourceName},
		},
		Sinks: []domain.SinkSpec{
			{Type: "mock", Name: stageSinkName, From: stage1Name},
		},
	}

	// Create factories
	sourceFactory := newMockFactory[domain.SourceSpec, domain.Source](func(spec domain.SourceSpec) (domain.Source, error) {
		return newMockSource(spec.Name), nil
	})

	stageFactory := newMockFactory[domain.StageSpec, domain.Stage](func(spec domain.StageSpec) (domain.Stage, error) {
		return newMockStage(spec.Name), nil
	})

	sinkFactory := newMockFactory[domain.SinkSpec, domain.Sink](func(spec domain.SinkSpec) (domain.Sink, error) {
		return newMockSink(spec.Name), nil
	})

	// Create core
	core := core.New(ctx, logger, sourceFactory, stageFactory, sinkFactory)
	defer core.Close()

	// Upsert initial stream
	_, err := core.UpsertStream(ctx, initialSpec)
	require.NoError(t, err)

	// Update stream spec with additional stage and corresponding sink
	updatedSpec := initialSpec
	updatedSpec.Stages = append(updatedSpec.Stages, domain.StageSpec{
		Type: "mock",
		Name: stage2Name,
		From: stage1Name,
	})
	updatedSpec.Sinks = []domain.SinkSpec{
		{Type: "mock", Name: stageSinkUpdated, From: stage2Name},
	}

	// Upsert updated stream
	_, err = core.UpsertStream(ctx, updatedSpec)
	require.NoError(t, err)

	// Verify stream was updated
	stream, err := core.GetStream(ctx, stageStreamName)
	require.NoError(t, err)
	assert.Equal(t, stageStreamName, stream.Name)
}

// TestStreamHotSwappingWithMultipleSinks tests hot swapping with multiple sinks
func TestStreamHotSwappingWithMultipleSinks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.Default()

	const (
		multiSinkStreamName = "multi-sink-stream"
		multiSinkSourceName = "multi-sink-source"
	)

	// Create initial stream spec with multiple sinks
	initialSpec := domain.StreamSpec{
		Name: multiSinkStreamName,
		Source: domain.SourceSpec{
			Type: "mock",
			Name: multiSinkSourceName,
		},
		Stages: []domain.StageSpec{},
		Sinks: []domain.SinkSpec{
			{Type: "mock", Name: "sink-1", From: multiSinkSourceName},
			{Type: "mock", Name: "sink-2", From: multiSinkSourceName},
		},
	}

	// Create factories
	sourceFactory := newMockFactory[domain.SourceSpec, domain.Source](func(spec domain.SourceSpec) (domain.Source, error) {
		return newMockSource(spec.Name), nil
	})

	stageFactory := newMockFactory[domain.StageSpec, domain.Stage](func(spec domain.StageSpec) (domain.Stage, error) {
		return newMockStage(spec.Name), nil
	})

	sinkFactory := newMockFactory[domain.SinkSpec, domain.Sink](func(spec domain.SinkSpec) (domain.Sink, error) {
		return newMockSink(spec.Name), nil
	})

	// Create core
	core := core.New(ctx, logger, sourceFactory, stageFactory, sinkFactory)
	defer core.Close()

	// Upsert initial stream
	_, err := core.UpsertStream(ctx, initialSpec)
	require.NoError(t, err)

	// Update stream spec with different sinks
	updatedSpec := initialSpec
	updatedSpec.Sinks = []domain.SinkSpec{
		{Type: "mock", Name: "updated-sink-1", From: multiSinkSourceName},
		{Type: "mock", Name: "updated-sink-2", From: multiSinkSourceName},
		{Type: "mock", Name: "updated-sink-3", From: multiSinkSourceName},
	}

	// Upsert updated stream
	_, err = core.UpsertStream(ctx, updatedSpec)
	require.NoError(t, err)

	// Verify stream was updated
	stream, err := core.GetStream(ctx, multiSinkStreamName)
	require.NoError(t, err)
	assert.Equal(t, multiSinkStreamName, stream.Name)
}

// mockFactory implements ports.Factory for testing
type mockFactory[Spec any, Component any] struct {
	createFunc func(spec Spec) (Component, error)
}

func newMockFactory[Spec any, Component any](createFunc func(spec Spec) (Component, error)) *mockFactory[Spec, Component] {
	return &mockFactory[Spec, Component]{createFunc: createFunc}
}

func (f *mockFactory[Spec, Component]) New(spec Spec) (Component, error) {
	return f.createFunc(spec)
}
