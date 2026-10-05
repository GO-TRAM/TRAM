package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	Type = "nats"

	// SignalHeader carries the domain.Signal of a consumed payload. It must
	// match the header the nats sink writes.
	SignalHeader = "Tram-Signal"

	// redeliveryDelay keeps a failing downstream from being hammered with the
	// same message.
	redeliveryDelay = 5 * time.Second
)

const (
	// fetchTimeout bounds how long one fetch waits for messages. Stopping the
	// source cancels a pending fetch right away.
	fetchTimeout = 5 * time.Second

	// After a failed fetch, the source waits before the next one. The wait
	// grows by fetchRetryGrowth with every failure in a row, between these
	// bounds.
	minFetchRetryDelay = 100 * time.Millisecond
	maxFetchRetryDelay = 5 * time.Second
	fetchRetryGrowth   = 2
)

type NATS struct {
	*base.Source[Spec]

	logger *slog.Logger
	nc     *nats.Conn
	js     jetstream.JetStream
	// wg tracks the messages being handled.
	wg sync.WaitGroup

	// stop is cancelled by Stop and interrupts pending fetches.
	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*NATS, error) {
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(base.Spec().URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	stop, cancel := context.WithCancel(context.Background())
	return &NATS{
		Source: base,
		logger: logger,
		nc:     nc,
		js:     js,
		stop:   stop,
		cancel: cancel,
	}, nil
}

func (n *NATS) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.running = true
	n.mu.Unlock()
	// The connection is closed once the workers are done, so messages still
	// being processed can be acknowledged.
	defer n.nc.Close()

	stream, err := n.js.Stream(ctx, n.Spec().Stream)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", n.Spec().Stream, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       n.Spec().Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: n.Spec().Subject,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	workers := uint(1)
	if n.Spec().Workers > 0 {
		workers = n.Spec().Workers
	}

	n.consume(ctx, cons, int(workers))
	return nil
}

// consume fetches messages until the source is stopped and handles each in
// its own goroutine, at most workers at a time. Only as many messages are
// fetched as slots are free, so no fetched message waits for another to be
// handled while its ack wait runs. A stage that holds messages, like a batch,
// therefore holds up to workers of them.
//
// Fetches and the wait after a failed fetch are interrupted by Stop;
// deliveries use ctx, so messages being handled when the source stops are
// still delivered and acknowledged. consume returns once they are done.
func (n *NATS) consume(ctx context.Context, cons jetstream.Consumer, workers int) {
	defer n.wg.Wait()

	slots := make(chan struct{}, workers)
	var delay time.Duration
	for {
		free := n.acquire(slots)
		if free == 0 {
			return
		}
		err := n.fetch(ctx, cons, slots, free)
		if err == nil || n.stop.Err() != nil {
			delay = 0
			continue
		}

		// Failing fetches often fail right away, e.g. while disconnected, so
		// the source waits before the next one instead of spinning.
		delay = min(max(fetchRetryGrowth*delay, minFetchRetryDelay), maxFetchRetryDelay)
		n.logger.ErrorContext(ctx, "Could not fetch messages, retrying",
			slog.String("source", n.Name()), slog.Duration("delay", delay), slog.Any("error", err))

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-n.stop.Done():
			timer.Stop()
		}
	}
}

// acquire waits until at least one slot is free and takes all free slots. It
// returns how many it took, or 0 once the source is stopped.
func (n *NATS) acquire(slots chan struct{}) int {
	// With a slot free, select would pick at random between it and a stop
	// that already happened, and fetch again.
	if n.stop.Err() != nil {
		return 0
	}
	select {
	case slots <- struct{}{}:
	case <-n.stop.Done():
		return 0
	}
	free := 1
	for {
		select {
		case slots <- struct{}{}:
			free++
		default:
			return free
		}
	}
}

// release frees count slots.
func release(slots chan struct{}, count int) {
	for range count {
		<-slots
	}
}

// fetch fetches up to free messages, one per slot taken, and hands each to
// its own goroutine, which frees its slot once the message is handled. Slots
// of messages the fetch did not return are freed when it ends. It returns the
// error of a failed fetch, which is reported either right away or once the
// fetch has ended.
func (n *NATS) fetch(ctx context.Context, cons jetstream.Consumer, slots chan struct{}, free int) error {
	fetchCtx, cancel := context.WithTimeout(n.stop, fetchTimeout)
	defer cancel()

	msgs, err := cons.Fetch(free, jetstream.FetchContext(fetchCtx))
	if err != nil {
		release(slots, free)
		return err
	}
	received := 0
	for msg := range msgs.Messages() {
		received++
		n.wg.Go(func() {
			defer release(slots, 1)
			n.handle(ctx, msg)
		})
	}
	release(slots, free-received)

	// A fetch that ends because the wait is over or the source stops did not
	// fail.
	err = msgs.Error()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (n *NATS) handle(ctx context.Context, msg jetstream.Msg) {
	signal := domain.Signal(msg.Headers().Get(SignalHeader))
	event, err := domain.UnmarshalOTLPData(signal, msg.Data())
	if err != nil {
		// Redelivery cannot fix a message that does not decode.
		n.logger.WarnContext(ctx, "Terminated message that is not valid OTLP data", slog.Any("error", err))
		n.term(ctx, msg)
		return
	}

	// Delivery is at-least-once: when one branch fails, the redelivered
	// message reaches all branches again.
	err = n.NotifySubscribers(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		n.logger.WarnContext(ctx, "Terminated rejected message", slog.Any("error", err))
		n.term(ctx, msg)
		return
	}
	if err != nil {
		n.logger.WarnContext(ctx, "Could not deliver message, requesting redelivery", slog.Any("error", err))
		if err = msg.NakWithDelay(redeliveryDelay); err != nil {
			n.logger.ErrorContext(ctx, "Could not request redelivery of message", slog.Any("error", err))
		}
		return
	}

	if err = msg.Ack(); err != nil {
		n.logger.ErrorContext(ctx, "Could not acknowledge message", slog.Any("error", err))
	}
}

// term terminates a message, so it is not delivered again.
func (n *NATS) term(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		n.logger.ErrorContext(ctx, "Could not terminate message", slog.Any("error", err))
	}
}

// Stop interrupts pending fetches; Run returns once messages being processed
// are done. Stop may be called without Run, e.g. when building the stream
// failed, and then closes the connection itself.
func (n *NATS) Stop() error {
	n.mu.Lock()
	n.stopped = true
	running := n.running
	n.mu.Unlock()

	n.cancel()
	if !running {
		n.nc.Close()
	}
	return nil
}
