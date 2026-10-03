package debug

import (
	"context"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"codeberg.org/eljoth/tram/internal/adapters/sink/base"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const Type = "debug"

type Debug struct {
	*base.Sink[Spec]

	logger *slog.Logger
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*Debug, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Debug{
		Sink:   base,
		logger: logger,
	}, nil
}

func (d *Debug) Notify(ctx context.Context, event *domain.Event) error {
	path, _ := ctx.Value(domain.ContextKeyPath).(string)
	path += " -> " + d.Name()
	d.logger.InfoContext(ctx, "Debug event",
		slog.String("path", path), slog.Int("records", event.RecordCount()), slog.Any("event", event))
	return nil
}
