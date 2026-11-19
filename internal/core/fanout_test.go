// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/testutil/otlpgen"
)

// recorder is a branch of a fanout that records the events it receives. If
// notify is set, it runs first and an error from it is returned instead.
type recorder struct {
	name   string
	notify func(context.Context, *domain.Event) error

	mu     sync.Mutex
	events []*domain.Event
}

func (r *recorder) Name() string { return r.name }
func (r *recorder) From() string { return "" }
func (r *recorder) Stop() error  { return nil }

func (r *recorder) Notify(ctx context.Context, event *domain.Event) error {
	if r.notify != nil {
		if err := r.notify(ctx, event); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recorder) received() []*domain.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*domain.Event(nil), r.events...)
}

func recorders(n int) ([]*recorder, []domain.Subscriber) {
	recs := make([]*recorder, n)
	subs := make([]domain.Subscriber, n)
	for i := range recs {
		recs[i] = &recorder{name: string(rune('a' + i))}
		subs[i] = recs[i]
	}
	return recs, subs
}

func logEvent() *domain.Event {
	return &domain.Event{Logs: otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1, Teams: 1})}
}

func TestFanoutDeliversToBranchesConcurrently(t *testing.T) {
	t.Parallel()

	// Every branch waits until all branches have the event. Delivering one
	// branch after the other would never get past the first.
	const branches = 3
	var arrived sync.WaitGroup
	arrived.Add(branches)
	all := make(chan struct{})
	go func() { arrived.Wait(); close(all) }()

	recs, subs := recorders(branches)
	for _, r := range recs {
		r.notify = func(context.Context, *domain.Event) error {
			arrived.Done()
			select {
			case <-all:
				return nil
			case <-time.After(5 * time.Second):
				return errors.New("the other branches did not get the event at the same time")
			}
		}
	}

	require.NoError(t, core.NewFanout("fanout", subs).Notify(t.Context(), logEvent()))
}

func TestFanoutSharesTheEventWithEveryBranch(t *testing.T) {
	t.Parallel()

	recs, subs := recorders(3)
	event := logEvent()
	require.NoError(t, core.NewFanout("fanout", subs).Notify(t.Context(), event))

	for _, r := range recs {
		got := r.received()
		require.Len(t, got, 1, r.name)
		assert.Same(t, event, got[0], "branch %s gets the event itself, not a copy", r.name)
	}
}

func TestFanoutFailingBranchDoesNotStopTheOthers(t *testing.T) {
	t.Parallel()

	errBranch := errors.New("branch failed")
	recs, subs := recorders(3)
	recs[1].notify = func(context.Context, *domain.Event) error { return errBranch }

	err := core.NewFanout("fanout", subs).Notify(t.Context(), logEvent())

	require.ErrorIs(t, err, errBranch, "the error of the branch reaches the source")
	assert.Len(t, recs[0].received(), 1, "the branch before the failing one gets the event")
	assert.Len(t, recs[2].received(), 1, "the branch after the failing one gets the event")
	assert.Empty(t, recs[1].received())
}

// A source may reuse or release an event once Notify returns, so no branch
// may still be working on it: a failing branch does not end Notify early.
func TestFanoutReturnsOnlyAfterEveryBranch(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	var slowDone atomic.Bool
	recs, subs := recorders(2)
	recs[0].notify = func(context.Context, *domain.Event) error { return errors.New("failed at once") }
	recs[1].notify = func(context.Context, *domain.Event) error {
		<-release
		slowDone.Store(true)
		return nil
	}

	returned := make(chan error, 1)
	go func() { returned <- core.NewFanout("fanout", subs).Notify(t.Context(), logEvent()) }()

	select {
	case <-returned:
		t.Fatal("Notify returned while a branch was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.Error(t, <-returned)
	assert.True(t, slowDone.Load(), "the slow branch finished before Notify returned")
}

func TestFanoutKeepsTheOrderOfEventsPerBranch(t *testing.T) {
	t.Parallel()

	recs, subs := recorders(3)
	fanout := core.NewFanout("fanout", subs)
	events := make([]*domain.Event, 50)
	for i := range events {
		events[i] = logEvent()
		require.NoError(t, fanout.Notify(t.Context(), events[i]))
	}

	for _, r := range recs {
		got := r.received()
		require.Len(t, got, len(events), r.name)
		for i := range events {
			assert.Same(t, events[i], got[i], "branch %s, event %d", r.name, i)
		}
	}
}

func TestFanoutWithOneBranchPassesTheErrorOn(t *testing.T) {
	t.Parallel()

	errBranch := errors.New("branch failed")
	recs, subs := recorders(1)
	recs[0].notify = func(context.Context, *domain.Event) error { return errBranch }

	assert.Same(t, errBranch, core.NewFanout("fanout", subs).Notify(t.Context(), logEvent()))
}

func TestFanoutWithoutBranches(t *testing.T) {
	t.Parallel()

	assert.NoError(t, core.NewFanout("fanout", nil).Notify(t.Context(), logEvent()))
}
