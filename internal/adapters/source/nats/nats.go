package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"codeberg.org/eljoth/tram/internal/adapters/source/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	fetchBatch = 10
	// fetchTimeout bounds how long one fetch waits for messages. Stopping the
	// source cancels a pending fetch right away.
	fetchTimeout = 5 * time.Second
)

type NATS struct {
	*base.Source[Spec]
	nc *nats.Conn
	js jetstream.JetStream
	wg sync.WaitGroup

	// stop is cancelled by Stop and interrupts pending fetches.
	stop   context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	stopped bool
}

func New(source domain.SourceSpec, validator *validator.Validate) (*NATS, error) {
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

	for range workers {
		n.wg.Go(func() { n.worker(ctx, cons) })
	}

	<-n.stop.Done()
	n.wg.Wait()
	return nil
}

// worker fetches messages until the source is stopped. Fetches are
// interrupted by Stop; deliveries use ctx, so a message being processed when
// the source stops is still delivered and acknowledged.
func (n *NATS) worker(ctx context.Context, cons jetstream.Consumer) {
	for n.stop.Err() == nil {
		fetchCtx, cancel := context.WithTimeout(n.stop, fetchTimeout)
		msgs, err := cons.Fetch(fetchBatch, jetstream.FetchContext(fetchCtx))
		if err != nil {
			cancel()
			if n.stop.Err() == nil {
				log.Printf("Error fetching messages: %v", err)
			}
			continue
		}

		for msg := range msgs.Messages() {
			n.handle(ctx, msg)
		}
		cancel()
	}
}

func (n *NATS) handle(ctx context.Context, msg jetstream.Msg) {
	signal := domain.Signal(msg.Headers().Get(SignalHeader))
	event, err := domain.UnmarshalOTLPData(signal, msg.Data())
	if err != nil {
		// Redelivery cannot fix a message that does not decode.
		log.Printf("Error parsing OTLP data, terminating message: %v", err)
		if err := msg.Term(); err != nil {
			log.Printf("Error terminating message: %v", err)
		}
		return
	}

	// Delivery is at-least-once: when one branch fails, the redelivered
	// message reaches all branches again.
	err = n.NotifySubscribers(ctx, event)
	if errors.Is(err, domain.ErrEventRejected) {
		log.Printf("Message rejected, terminating it: %v", err)
		if err := msg.Term(); err != nil {
			log.Printf("Error terminating message: %v", err)
		}
		return
	}
	if err != nil {
		log.Printf("Error notifying subscribers, requesting redelivery: %v", err)
		if err := msg.NakWithDelay(redeliveryDelay); err != nil {
			log.Printf("Error rejecting message: %v", err)
		}
		return
	}

	if err := msg.Ack(); err != nil {
		log.Printf("Error acknowledging message: %v", err)
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
