// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

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

const streamSetUpAttempts = 3

var errSubjectsOverwritten = errors.New("subjects were removed by a concurrent update of the stream")

func setUpStream(
	ctx context.Context,
	js jetstream.JetStream,
	spec *Spec,
	logger *slog.Logger,
) (jetstream.Stream, error) {
	var err error
	for range streamSetUpAttempts {
		if err = applyStream(ctx, js, spec, logger); err != nil {
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
	retention, _, err := retentionPolicyFromSpec(spec)
	if err != nil {
		return jetstream.StreamConfig{}, err
	}
	maxAge, err := maxAgeFromSpec(spec)
	if err != nil {
		return jetstream.StreamConfig{}, err
	}
	compression, _ := compressionFromSpec(spec)
	return jetstream.StreamConfig{
		Name:        spec.Stream,
		Subjects:    spec.Subject.Register,
		Retention:   retention,
		MaxAge:      maxAge,
		MaxBytes:    spec.Size,
		Compression: compression,
	}, nil
}

func compressionFromSpec(spec *Spec) (jetstream.StoreCompression, bool) {
	switch spec.Compression {
	case "s2":
		return jetstream.S2Compression, true
	case "none":
		return jetstream.NoCompression, true
	default:
		return jetstream.NoCompression, false
	}
}

type streamUpdate struct {
	subjects       []string
	limitsChanged  bool
	replacedLimits []string
}

func (u streamUpdate) changed() bool {
	return len(u.subjects) > 0 || u.limitsChanged
}

// applyLimit records that a limit of the spec is applied; replaced describes
// the different, previously set limit it replaces, if any.
func applyLimit(u *streamUpdate, replaced string) {
	u.limitsChanged = true
	if replaced != "" {
		u.replacedLimits = append(u.replacedLimits, replaced)
	}
}

func mergeStreamConfig(existing jetstream.StreamConfig, spec *Spec) (jetstream.StreamConfig, streamUpdate, error) {
	retention, retentionSet, err := retentionPolicyFromSpec(spec)
	if err != nil {
		return existing, streamUpdate{}, err
	}
	maxAge, err := maxAgeFromSpec(spec)
	if err != nil {
		return existing, streamUpdate{}, err
	}

	merged := existing
	merged.Subjects = slices.Clone(existing.Subjects)
	var update streamUpdate

	for _, subject := range spec.Subject.Register {
		if !covered(merged.Subjects, []string{subject}) {
			merged.Subjects = append(merged.Subjects, subject)
			update.subjects = append(update.subjects, subject)
		}
	}

	if maxAge != 0 && merged.MaxAge != maxAge {
		applyLimit(&update, replacedIf(merged.MaxAge > 0, "max_age %s -> %s", merged.MaxAge, maxAge))
		merged.MaxAge = maxAge
	}
	if retentionSet && merged.Retention != retention {
		applyLimit(&update, replacedIf(merged.Retention != jetstream.LimitsPolicy, "retention %s -> %s",
			strings.ToLower(merged.Retention.String()), strings.ToLower(retention.String())))
		merged.Retention = retention
	}
	if spec.Size != 0 && merged.MaxBytes != spec.Size {
		applyLimit(&update, replacedIf(merged.MaxBytes > 0, "size %d -> %d", merged.MaxBytes, spec.Size))
		merged.MaxBytes = spec.Size
	}
	if compression, ok := compressionFromSpec(spec); ok && merged.Compression != compression {
		applyLimit(&update, replacedIf(merged.Compression != jetstream.NoCompression, "compression %s -> %s",
			strings.ToLower(merged.Compression.String()), spec.Compression))
		merged.Compression = compression
	}

	return merged, update, nil
}

// replacedIf describes a replaced limit if the stream had one set before.
func replacedIf(wasSet bool, format string, args ...any) string {
	if !wasSet {
		return ""
	}
	return fmt.Sprintf(format, args...)
}

// retentionPolicyFromSpec returns the retention policy of the spec and whether
// it sets one. LimitsPolicy is the zero value, so it cannot tell by itself.
func retentionPolicyFromSpec(spec *Spec) (jetstream.RetentionPolicy, bool, error) {
	switch spec.Retention {
	case "":
		return jetstream.LimitsPolicy, false, nil
	case "limits":
		return jetstream.LimitsPolicy, true, nil
	case "interest":
		return jetstream.InterestPolicy, true, nil
	case "workqueue":
		return jetstream.WorkQueuePolicy, true, nil
	default:
		return 0, false, fmt.Errorf(
			"invalid retention policy: %q (must be 'limits', 'interest', or 'workqueue')",
			spec.Retention,
		)
	}
}

func maxAgeFromSpec(spec *Spec) (time.Duration, error) {
	if spec.MaxAge == "" {
		return 0, nil
	}
	maxAge, err := time.ParseDuration(spec.MaxAge)
	if err != nil {
		return 0, fmt.Errorf("invalid max_age: %w", err)
	}
	return maxAge, nil
}

func covered(patterns, subjects []string) bool {
	for _, subject := range subjects {
		if !slices.ContainsFunc(patterns, func(pattern string) bool { return subjectCovers(pattern, subject) }) {
			return false
		}
	}
	return true
}

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
			return false
		case p == "*":
		case p != s || s == "*":
			return false
		}
	}
	return len(patternTokens) == len(subjectTokens)
}
