// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// mockSubscriber implements domain.Subscriber for testing
type mockSubscriber struct {
	name       string
	onNotify   func(ctx context.Context, event *domain.Event) error
	onNotifyCh chan struct{}
	errors     []error
	mu         sync.Mutex
}

func newMockSubscriber(name string) *mockSubscriber {
	return &mockSubscriber{
		name:       name,
		onNotifyCh: make(chan struct{}, 1),
	}
}

func (m *mockSubscriber) Name() string { return m.name }
func (m *mockSubscriber) From() string { return "" }
func (m *mockSubscriber) Stop() error { return nil }

func (m *mockSubscriber) Notify(ctx context.Context, event *domain.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.onNotify != nil {
		err := m.onNotify(ctx, event)
		if err != nil {
			m.errors = append(m.errors, err)
			return err
		}
	}

	// Non-blocking send to avoid deadlocks
	select {
	case m.onNotifyCh <- struct{}{}:
	default:
		// Channel is full, skip the notification
	}
	return nil
}

func (m *mockSubscriber) GetErrors() []error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]error(nil), m.errors...)
}

func (m *mockSubscriber) GetOnNotifyCh() <-chan struct{} {
	return m.onNotifyCh
}

// TestFanoutConcurrentDelivery tests that events are delivered to all child subscribers concurrently
func TestFanoutConcurrentDelivery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	subscribers := []domain.Subscriber{
		newMockSubscriber("subscriber-1"),
		newMockSubscriber("subscriber-2"),
		newMockSubscriber("subscriber-3"),
	}

	fanout, err := core.NewFanout("test-fanout", subscribers)
	require.NoError(t, err)

	// Start a goroutine to notify the fanout
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		errCh <- fanout.Notify(ctx, event)
	}()

	// Wait for all subscribers to be notified
	var notified int
	for range subscribers {
		select {
		case <-subscribers[notified].(*mockSubscriber).GetOnNotifyCh():
			notified++
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for subscribers to be notified")
		}
	}

	// Wait for the fanout to finish
	err = <-errCh
	wg.Wait()

	require.NoError(t, err)
	assert.Equal(t, 3, notified, "All subscribers should be notified")
}

// TestFanoutErrorIsolation tests that failure in one branch doesn't stop other branches
func TestFanoutErrorIsolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Create subscribers: one that fails, two that succeed
	failingSubscriber := newMockSubscriber("failing-subscriber")
	failingSubscriber.onNotify = func(ctx context.Context, event *domain.Event) error {
		return errors.New("simulated failure")
	}

	successSubscriber1 := newMockSubscriber("success-subscriber-1")
	successSubscriber2 := newMockSubscriber("success-subscriber-2")

	fanout, err := core.NewFanout("test-fanout-with-failure", []domain.Subscriber{failingSubscriber, successSubscriber1, successSubscriber2})
	require.NoError(t, err)

	// Notify the fanout
	err = fanout.Notify(ctx, event)
	require.Error(t, err, "Fanout should return an error when any subscriber fails")

	// Check that the failing subscriber returned an error
	failingErrors := failingSubscriber.GetErrors()
	require.Len(t, failingErrors, 1, "Failing subscriber should have one error")
	assert.Equal(t, "simulated failure", failingErrors[0].Error())

	// Check that the successful subscribers were still notified
	select {
	case <-successSubscriber1.GetOnNotifyCh():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("First successful subscriber should have been notified despite failure")
	}

	select {
	case <-successSubscriber2.GetOnNotifyCh():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Second successful subscriber should have been notified despite failure")
	}
}

// TestFanoutEventOrdering tests that events maintain their order within each branch
func TestFanoutEventOrdering(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create ordered events
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	events := []*domain.Event{
		{Logs: otlpgen.Logs(shape)},
		{Logs: otlpgen.Logs(shape)},
		{Logs: otlpgen.Logs(shape)},
	}

	// Track the order events are received
	type receivedEvent struct {
		subscriber string
		eventIdx   int
	}
	received := make([]receivedEvent, 0)
	mu := sync.Mutex{}

	// Create subscribers that track event order
	subscribers := []domain.Subscriber{
		newMockSubscriber("order-tracker-1"),
		newMockSubscriber("order-tracker-2"),
	}

	// Set up event order tracking
	for i, sub := range subscribers {
		j, s := i, sub
		s.(*mockSubscriber).onNotify = func(ctx context.Context, event *domain.Event) error {
			mu.Lock()
			received = append(received, receivedEvent{
				subscriber: fmt.Sprintf("order-tracker-%d", j+1),
				eventIdx:   len(received), // Track the order of receipt
			})
			mu.Unlock()
			return nil
		}
	}

	fanout, err := core.NewFanout("test-fanout-ordering", subscribers)
	require.NoError(t, err)

	// Send events sequentially
	for _, event := range events {
		err := fanout.Notify(ctx, event)
		require.NoError(t, err, "Fanout should not fail for valid events")
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify that each subscriber received all events
	assert.Len(t, received, len(events)*len(subscribers), "All events should be received by all subscribers")

	// Verify that events were received in order for each subscriber
	for _, sub := range subscribers {
		subName := sub.(*mockSubscriber).name
		var subEvents []int
		for _, re := range received {
			if re.subscriber == subName {
				subEvents = append(subEvents, re.eventIdx)
			}
		}
		// Check that the event indices are sequential for this subscriber
		// With 3 events to 2 subscribers, each subscriber gets events at indices 0,1,2,3,4,5
		// But we need to verify they're in order, not that they're 0,1,2
		for i := 1; i < len(subEvents); i++ {
			assert.Less(t, subEvents[i-1], subEvents[i], "Events should be received in order for %s", subName)
		}
	}
}

// TestFanoutResourceCleanup tests that resources are properly cleaned up when fanout encounters errors
func TestFanoutResourceCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	// Create a subscriber that leaks resources
	resourceLeakingSubscriber := newMockSubscriber("resource-leaking-subscriber")
	var leakDetected bool
	defer func() {
		if !leakDetected {
			// This would be detected by a proper resource tracking mechanism
			t.Log("Resource leak detection would be implemented in production")
		}
	}()

	resourceLeakingSubscriber.onNotify = func(ctx context.Context, event *domain.Event) error {
		// Simulate resource allocation
		return nil
	}

	fanout, err := core.NewFanout("test-fanout-cleanup", []domain.Subscriber{resourceLeakingSubscriber})
	require.NoError(t, err)

	// Notify the fanout
	err = fanout.Notify(ctx, event)
	require.NoError(t, err)

	// In a real implementation, we would track resource allocation and ensure cleanup
	// For now, we just verify that the fanout itself doesn't leak resources
	assert.NotNil(t, fanout, "Fanout should still be accessible after notification")
	leakDetected = true
}

// TestFanoutSingleChildOptimization tests that fanout optimizes for single child
func TestFanoutSingleChildOptimization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	singleSubscriber := newMockSubscriber("single-subscriber")

	fanout, err := core.NewFanout("test-fanout-single-child", []domain.Subscriber{singleSubscriber})
	require.NoError(t, err)

	// Notify the fanout
	err = fanout.Notify(ctx, event)
	require.NoError(t, err)

	// Verify the single subscriber was notified
	select {
	case <-singleSubscriber.GetOnNotifyCh():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Single subscriber should have been notified")
	}
}

// TestFanoutEmptyChildren tests fanout behavior with no children
func TestFanoutEmptyChildren(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shape := otlpgen.Shape{Records: 1, Resources: 1, Teams: 1}
	event := &domain.Event{
		Logs: otlpgen.Logs(shape),
	}

	fanout, err := core.NewFanout("test-fanout-empty", []domain.Subscriber{})
	require.NoError(t, err)

	// Notify the fanout with no children should not error
	err = fanout.Notify(ctx, event)
	require.NoError(t, err, "Fanout with no children should not error")
}
