package configsync_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/adapters/configsync"
	"codeberg.org/eljoth/tram/internal/adapters/sink"
	"codeberg.org/eljoth/tram/internal/adapters/source"
	"codeberg.org/eljoth/tram/internal/adapters/stage"
	"codeberg.org/eljoth/tram/internal/core"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

// streamFile is a stream file whose generator emits nothing during a test.
const streamFile = `{
  "stream": {
    "source": {"name": "generator", "type": "generator", "spec": {"interval": "1h"}},
    "sinks": [{"name": "debug", "type": "debug", "from": "generator"}]
  }
}`

func newCore(t *testing.T) *core.Core {
	t.Helper()

	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	return c
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

// run starts the sync and stops it when the test ends.
func run(t *testing.T, cs *configsync.ConfigSync) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- cs.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
		assert.NoError(t, cs.Close())
	})
}

func TestStreamNameCannotEscapeDirectory(t *testing.T) {
	t.Parallel()

	// dir is nested, so a file escaping it would land in parent.
	parent := t.TempDir()
	dir := filepath.Join(parent, "config")
	require.NoError(t, os.Mkdir(dir, 0o700))

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	// As a client of the API could send it.
	_, err = c.UpsertStream(t.Context(), domain.StreamSpec{
		Name:   "../escape",
		Source: domain.SourceSpec{Name: "generator", Type: "generator", Spec: map[string]any{"interval": "1h"}},
		Sinks:  []domain.SinkSpec{{Name: "debug", Type: "debug", From: "generator"}},
	})
	require.ErrorIs(t, err, domain.ErrInvalidStreamName)

	_, err = os.Stat(filepath.Join(parent, "escape.json"))
	assert.ErrorIs(t, err, os.ErrNotExist, "file written outside the config directory")
}

// Editors write swap and backup files next to the file they save. Those must
// not cancel the sync of the stream file.
func TestOtherFilesDoNotCancelSync(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "edited.json", streamFile)

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	writeFile(t, dir, "edited.json", strings.Replace(streamFile, "1h", "2h", 1))
	writeFile(t, dir, ".edited.json.swp", "swap")

	require.Eventually(t, func() bool {
		stream, getErr := c.GetStream(t.Context(), "edited")
		return getErr == nil && stream.Source.Spec["interval"] == "2h"
	}, 5*time.Second, 10*time.Millisecond, "the change to the stream file was not applied")
}

func TestUnreadableFileKeepsStream(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "edited.json", streamFile)
	writeFile(t, dir, "removed.json", streamFile)

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	_, err = c.GetStream(t.Context(), "edited")
	require.NoError(t, err)

	// A half-saved file, as an editor can leave it, is broken before the
	// other file is removed, so the sync that deletes the removed stream sees
	// it.
	const broken = `{"stream": {"source": `
	writeFile(t, dir, "edited.json", broken)
	require.NoError(t, os.Remove(filepath.Join(dir, "removed.json")))

	// Streams without a file are still deleted, which also shows the sync ran.
	require.Eventually(t, func() bool {
		_, getErr := c.GetStream(t.Context(), "removed")
		return getErr != nil
	}, 5*time.Second, 10*time.Millisecond)

	_, err = c.GetStream(t.Context(), "edited")
	require.NoError(t, err, "stream with an unreadable file was deleted")

	content, err := os.ReadFile(filepath.Join(dir, "edited.json"))
	require.NoError(t, err, "unreadable stream file was removed")
	assert.Equal(t, broken, string(content)) //nolint:testifylint // Invalid JSON on purpose; compared byte for byte.

	// Once the file is fixed, the stream follows it again.
	writeFile(t, dir, "edited.json", strings.Replace(streamFile, "1h", "2h", 1))
	require.Eventually(t, func() bool {
		stream, getErr := c.GetStream(t.Context(), "edited")
		return getErr == nil && stream.Source.Spec["interval"] == "2h"
	}, 5*time.Second, 10*time.Millisecond)

	_, err = c.GetStream(t.Context(), "removed")
	assert.ErrorIs(t, err, domain.ErrStreamNotFound)
}
