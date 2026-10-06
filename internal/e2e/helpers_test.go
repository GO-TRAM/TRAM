package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

const isCustomerA = "record.attributes.exists(a, a.key == 'customer' && a.value.string_value == 'A')"

func discard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

const captureType = "capture"

type captureSink struct {
	name, from string
	fail       string

	mu     sync.Mutex
	events []*domain.Event
}

func (s *captureSink) Name() string { return s.name }
func (s *captureSink) From() string { return s.from }
func (s *captureSink) Stop() error  { return nil }

func (s *captureSink) Notify(_ context.Context, e *domain.Event) error {
	switch s.fail {
	case "temporary":
		return errors.New("sink unavailable")
	case "permanent":
		return fmt.Errorf("%w: sink refused data", domain.ErrEventRejected)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *captureSink) received() []*domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*domain.Event(nil), s.events...)
}

type captureFactory struct {
	real ports.Factory[domain.SinkSpec, domain.Sink]

	mu    sync.Mutex
	sinks map[string]*captureSink
}

func (f *captureFactory) New(spec domain.SinkSpec) (domain.Sink, error) {
	if spec.Type != captureType {
		return f.real.New(spec)
	}
	fail, _ := spec.Spec["fail"].(string)
	s := &captureSink{name: spec.Name, from: spec.From, fail: fail}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sinks[spec.Name] = s
	return s, nil
}

func (f *captureFactory) sink(t *testing.T, name string) *captureSink {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sinks[name]
	require.True(t, ok, "sink %q was not created", name)
	return s
}

type pipeline struct {
	core    *core.Core
	sinks   *captureFactory
	in      domain.SourceSpec
	baseURL string
}

func newPipeline(t *testing.T, stages []domain.StageSpec, sinks []domain.SinkSpec) *pipeline {
	t.Helper()
	port := freePort(t)
	p := newStream(
		t,
		domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)}},
		stages,
		sinks,
	)
	p.baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	return p
}

func newStream(t *testing.T, in domain.SourceSpec, stages []domain.StageSpec, sinks []domain.SinkSpec) *pipeline {
	t.Helper()
	p := &pipeline{
		sinks: &captureFactory{real: sink.NewFactory(discard()), sinks: map[string]*captureSink{}},
		in:    in,
	}
	p.core = core.New(context.Background(), discard(), source.NewFactory(discard()), stage.NewFactory(), p.sinks)
	t.Cleanup(func() { _ = p.core.Close() })
	p.update(t, stages, sinks)
	return p
}

func (p *pipeline) update(t *testing.T, stages []domain.StageSpec, sinks []domain.SinkSpec) {
	t.Helper()
	_, err := p.core.UpsertStream(
		context.Background(),
		domain.StreamSpec{Name: "e2e", Source: p.in, Stages: stages, Sinks: sinks},
	)
	require.NoError(t, err)
}

func (p *pipeline) post(t *testing.T, logs *logspb.LogsData, timeout time.Duration) int {
	t.Helper()
	return p.postTo(t, "/v1/logs", logs, timeout)
}

func (p *pipeline) postTo(t *testing.T, path string, payload proto.Message, timeout time.Duration) int {
	t.Helper()
	body, err := proto.Marshal(payload)
	require.NoError(t, err)
	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(p.baseURL+path, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func (p *pipeline) postConcurrently(t *testing.T, requests ...*logspb.LogsData) []int {
	t.Helper()
	codes := make([]int, len(requests))
	var wg sync.WaitGroup
	for i, r := range requests {
		wg.Go(func() { codes[i] = p.post(t, r, 10*time.Second) })
	}
	wg.Wait()
	return codes
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type rec struct{ id, customer string }

func request(service string, records ...rec) *logspb.LogsData {
	sl := &logspb.ScopeLogs{Scope: &commonpb.InstrumentationScope{Name: "e2e"}}
	for _, r := range records {
		sl.LogRecords = append(sl.LogRecords, &logspb.LogRecord{
			Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: r.id}},
			Attributes: []*commonpb.KeyValue{stringAttr("customer", r.customer)},
		})
	}
	return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", service)}},
		ScopeLogs: []*logspb.ScopeLogs{sl},
	}}}
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func arrived(events []*domain.Event) []string {
	var out []string
	for _, e := range events {
		for _, rl := range e.Logs.GetResourceLogs() {
			service := ""
			for _, a := range rl.GetResource().GetAttributes() {
				if a.GetKey() == "service.name" {
					service = a.GetValue().GetStringValue()
				}
			}
			for _, sl := range rl.GetScopeLogs() {
				for _, r := range sl.GetLogRecords() {
					out = append(
						out,
						fmt.Sprintf("%s/%s/%s", service, sl.GetScope().GetName(), r.GetBody().GetStringValue()),
					)
				}
			}
		}
	}
	return out
}

func captureSinkSpec(name, from string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: captureType, From: from}
}

func customerFilter(name, from, expression string) domain.StageSpec {
	return domain.StageSpec{Name: name, Type: "cel_filter", From: from, Spec: map[string]any{"logs": expression}}
}
