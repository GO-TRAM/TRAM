package nats_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natssource "codeberg.org/eljoth/tram/internal/adapters/source/nats"
)

// fakeConsumer answers every fetch with the result of fetch.
type fakeConsumer struct {
	jetstream.Consumer

	fetches atomic.Int64
	fetch   func() (jetstream.MessageBatch, error)
}

func (c *fakeConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.fetches.Add(1)
	return c.fetch()
}

// endedBatch is a batch without messages that ended with err.
type endedBatch struct{ err error }

func (b endedBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}

func (b endedBatch) Error() error { return b.err }

// runWorker runs one worker against cons for d and returns what it logged.
func runWorker(t *testing.T, cons *fakeConsumer, d time.Duration) string {
	t.Helper()

	var logs bytes.Buffer
	src, err := natssource.NewWithoutConnection(slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		natssource.RunWorker(t.Context(), src, cons)
	}()

	time.Sleep(d)
	natssource.CancelFetching(src)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	return logs.String()
}

// Failing fetches often fail right away, e.g. while disconnected. The worker
// has to wait between them instead of spinning, whether the fetch fails
// directly or its batch ends with an error.
func TestWorkerBacksOffAfterFailedFetches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		fetch func() (jetstream.MessageBatch, error)
	}{
		{
			name:  "fetch fails",
			fetch: func() (jetstream.MessageBatch, error) { return nil, nats.ErrConnectionClosed },
		},
		{
			name: "batch ends with an error",
			fetch: func() (jetstream.MessageBatch, error) {
				return endedBatch{err: errors.New("consumer deleted")}, nil
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cons := &fakeConsumer{fetch: c.fetch}
			logs := runWorker(t, cons, 350*time.Millisecond)

			// Waits of 100ms and 200ms allow three fetches in 350ms.
			assert.LessOrEqual(t, cons.fetches.Load(), int64(4), "the worker spins")
			assert.GreaterOrEqual(t, cons.fetches.Load(), int64(2), "the worker gives up")
			assert.Contains(t, logs, "Could not fetch messages")
		})
	}
}

// A batch that ends because its wait is over is no failure: the worker fetches
// again right away and logs nothing.
func TestWorkerDoesNotBackOffAfterTimeout(t *testing.T) {
	t.Parallel()

	cons := &fakeConsumer{fetch: func() (jetstream.MessageBatch, error) {
		return endedBatch{err: context.DeadlineExceeded}, nil
	}}
	logs := runWorker(t, cons, 100*time.Millisecond)

	assert.Greater(t, cons.fetches.Load(), int64(10))
	assert.NotContains(t, logs, "Could not fetch messages")
}
