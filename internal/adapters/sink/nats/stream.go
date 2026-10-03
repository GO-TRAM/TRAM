package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// streamSetUpAttempts bounds how often setting up the stream is tried again
// when another sink changes it at the same time.
const streamSetUpAttempts = 3

var errSubjectsOverwritten = errors.New("subjects were removed by a concurrent update of the stream")

// setUpStream makes sure the JetStream stream exists and takes the sink's
// subjects. A missing stream is created from the spec. An existing stream is
// only extended: subjects it does not cover yet are added and limits set in
// the spec are applied, while everything else, like settings made by an
// operator or the subjects of other sinks, is kept.
func setUpStream(
	ctx context.Context,
	js jetstream.JetStream,
	spec *Spec,
	logger *slog.Logger,
) (jetstream.Stream, error) {
	var err error
	for range streamSetUpAttempts {
		if err = applyStream(ctx, js, spec, logger); err != nil {
			// Another sink created the stream meanwhile; extend it instead.
			if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
				continue
			}
			return nil, fmt.Errorf("failed to set up stream %s: %w", spec.Stream, err)
		}

		// JetStream has no compare-and-swap for stream configs, so a sink
		// updating the stream at the same time can overwrite the subjects
		// just added. Reading the config again detects that.
		stream, streamErr := js.Stream(ctx, spec.Stream)
		if streamErr != nil {
			return nil, fmt.Errorf("failed to get stream %s: %w", spec.Stream, streamErr)
		}
		if covered(stream.CachedInfo().Config.Subjects, spec.Subject.Register) {
			return stream, nil
		}
		err = errSubjectsOverwritten
	}
	return nil, fmt.Errorf("failed to set up stream %s: %w", spec.Stream, err)
}

// applyStream creates the stream or updates it with mergeStreamConfig. An
// existing stream that needs no change is not written to.
func applyStream(ctx context.Context, js jetstream.JetStream, spec *Spec, logger *slog.Logger) error {
	stream, err := js.Stream(ctx, spec.Stream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		config, configErr := configFromSpec(spec)
		if configErr != nil {
			return configErr
		}
		_, err = js.CreateStream(ctx, config)
		return err
	}
	if err != nil {
		return err
	}

	merged, update, err := mergeStreamConfig(stream.CachedInfo().Config, spec)
	if err != nil || !update.changed() {
		return err
	}

	if len(update.replacedLimits) > 0 {
		logger.WarnContext(ctx, "Replacing limits of JetStream stream; set them in one sink only",
			slog.String("stream", spec.Stream), slog.Any("limits", update.replacedLimits))
	}
	logger.InfoContext(ctx, "Updating JetStream stream",
		slog.String("stream", spec.Stream), slog.Any("added_subjects", update.subjects))
	_, err = js.UpdateStream(ctx, merged)
	return err
}

func configFromSpec(spec *Spec) (jetstream.StreamConfig, error) {
	maxAge, err := parseRetention(spec)
	if err != nil {
		return jetstream.StreamConfig{}, err
	}
	return jetstream.StreamConfig{
		Name:     spec.Stream,
		Subjects: spec.Subject.Register,
		MaxAge:   maxAge,
		MaxBytes: spec.Size,
	}, nil
}

// streamUpdate describes what mergeStreamConfig changed.
type streamUpdate struct {
	subjects      []string
	limitsChanged bool
	// replacedLimits lists limits of the spec that replaced a different,
	// previously set limit.
	replacedLimits []string
}

func (u streamUpdate) changed() bool {
	return len(u.subjects) > 0 || u.limitsChanged
}

// mergeStreamConfig returns the config the existing stream needs for the
// sink: subjects of the spec that the stream does not cover yet are added, and
// limits set in the spec are applied. Nothing is removed or reset.
func mergeStreamConfig(existing jetstream.StreamConfig, spec *Spec) (jetstream.StreamConfig, streamUpdate, error) {
	maxAge, err := parseRetention(spec)
	if err != nil {
		return existing, streamUpdate{}, err
	}

	merged := existing
	merged.Subjects = slices.Clone(existing.Subjects)
	var update streamUpdate

	for _, subject := range spec.Subject.Register {
		// The stream must not get overlapping subjects, which NATS rejects.
		if !covered(merged.Subjects, []string{subject}) {
			merged.Subjects = append(merged.Subjects, subject)
			update.subjects = append(update.subjects, subject)
		}
	}

	if maxAge != 0 && merged.MaxAge != maxAge {
		if merged.MaxAge > 0 {
			update.replacedLimits = append(update.replacedLimits,
				fmt.Sprintf("retention %s -> %s", merged.MaxAge, maxAge))
		}
		merged.MaxAge = maxAge
		update.limitsChanged = true
	}

	if spec.Size != 0 && merged.MaxBytes != spec.Size {
		if merged.MaxBytes > 0 {
			update.replacedLimits = append(update.replacedLimits,
				fmt.Sprintf("size %d -> %d", merged.MaxBytes, spec.Size))
		}
		merged.MaxBytes = spec.Size
		update.limitsChanged = true
	}

	return merged, update, nil
}

// parseRetention returns the retention of the spec, or 0 if it sets none.
func parseRetention(spec *Spec) (time.Duration, error) {
	if spec.Retention == "" {
		return 0, nil
	}
	retention, err := time.ParseDuration(spec.Retention)
	if err != nil {
		return 0, fmt.Errorf("invalid retention: %w", err)
	}
	return retention, nil
}

// covered reports whether every subject is matched by one of the patterns.
func covered(patterns, subjects []string) bool {
	for _, subject := range subjects {
		if !slices.ContainsFunc(patterns, func(pattern string) bool { return subjectCovers(pattern, subject) }) {
			return false
		}
	}
	return true
}

// subjectCovers reports whether pattern matches every subject that subject
// matches. Both may contain the NATS wildcards: "*" matches one token, ">"
// one or more trailing tokens.
func subjectCovers(pattern, subject string) bool {
	patternTokens := strings.Split(pattern, ".")
	subjectTokens := strings.Split(subject, ".")

	for i, p := range patternTokens {
		if p == ">" {
			return len(subjectTokens) > i
		}
		if i >= len(subjectTokens) {
			return false
		}
		s := subjectTokens[i]
		switch {
		case s == ">":
			// Only ">" covers any number of tokens.
			return false
		case p == "*":
		case p != s || s == "*":
			return false
		}
	}
	return len(patternTokens) == len(subjectTokens)
}
