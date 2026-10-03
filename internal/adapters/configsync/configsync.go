package configsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
)

const (
	// fsEventBuffer holds file system events while a sync runs.
	fsEventBuffer = 100
	// debounceDuration waits for a burst of file changes to settle.
	debounceDuration = 100 * time.Millisecond
	// filePermissions keeps stream files private; specs can contain
	// credentials, e.g. in NATS URLs.
	filePermissions = 0o600
	fileExtension   = ".json"
)

type ConfigSync struct {
	dir              string
	core             ports.Core
	logger           *slog.Logger
	watcher          *fsnotify.Watcher
	fsEvents         chan fsnotify.Event
	debouncer        *time.Timer
	debounceDuration time.Duration
}

func New(c ports.Core, dir string, logger *slog.Logger) (*ConfigSync, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	if err = watcher.Add(dir); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to watch directory %q: %w", dir, err), watcher.Close())
	}

	cs := &ConfigSync{
		dir:              dir,
		core:             c,
		logger:           logger,
		watcher:          watcher,
		fsEvents:         make(chan fsnotify.Event, fsEventBuffer),
		debounceDuration: debounceDuration,
	}

	if err = cs.sync(context.Background()); err != nil {
		return nil, errors.Join(err, watcher.Close())
	}

	return cs, nil
}

func (c *ConfigSync) Run(ctx context.Context) error {
	c.logger.InfoContext(ctx, "Starting configuration sync", slog.String("directory", c.dir))

	// Cancelling on return unregisters the watch, so the core never blocks
	// on a channel nobody reads anymore. Derived before the goroutine below
	// starts, which reads ctx.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for event := range c.watcher.Events {
			c.logger.DebugContext(ctx, "Received file system event",
				slog.String("path", event.Name), slog.String("op", event.Op.String()))
			c.fsEvents <- event
		}
	}()

	ch := make(chan domain.StreamSpecWatchEvent)
	if err := c.core.Watch(ctx, ch); err != nil {
		return fmt.Errorf("failed to watch core: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-c.watcher.Errors:
			return fmt.Errorf("watcher error: %w", err)

		case event := <-ch:
			if err := c.onCoreChange(event); err != nil {
				c.logger.ErrorContext(ctx, "Could not write stream change to file", slog.Any("error", err))
			}

		case event := <-c.fsEvents:
			if c.debouncer != nil {
				c.debouncer.Stop()
			}
			if filepath.Ext(event.Name) == fileExtension {
				c.debouncer = time.AfterFunc(c.debounceDuration, func() {
					if err := c.sync(ctx); err != nil {
						c.logger.ErrorContext(ctx, "Could not sync stream files", slog.Any("error", err))
					}
				})
			}
		}
	}
}

// sync makes the core's streams match the stream files: it creates or
// updates a stream for every file and deletes streams without a file. A
// stream whose file cannot be read is left as it is, because deleting it
// would also remove the file, e.g. while an editor is still saving it.
func (c *ConfigSync) sync(ctx context.Context) error {
	files, unreadable, err := c.readStreamFiles(ctx)
	if err != nil {
		return err
	}

	configured, err := c.core.GetStreams(ctx)
	if err != nil {
		return err
	}
	for _, stream := range configured {
		if _, ok := files[stream.Name]; ok {
			continue
		}
		if _, ok := unreadable[stream.Name]; ok {
			c.logger.WarnContext(ctx, "Keeping stream whose file cannot be read",
				slog.String("stream", stream.Name))
			continue
		}
		c.logger.DebugContext(ctx, "Deleting stream", slog.String("stream", stream.Name))
		if err = c.core.DeleteStream(ctx, stream.Name); err != nil {
			c.logger.ErrorContext(
				ctx,
				"Could not delete stream",
				slog.String("stream", stream.Name),
				slog.Any("error", err),
			)
		}
	}

	for name, file := range files {
		c.apply(ctx, name, file)
	}
	return nil
}

// readStreamFiles reads all stream files, keyed by stream name. The names of
// files that cannot be read are returned separately.
func (c *ConfigSync) readStreamFiles(ctx context.Context) (map[string]Stream, map[string]struct{}, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, nil, err
	}

	files := make(map[string]Stream, len(entries))
	unreadable := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != fileExtension {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), fileExtension)
		stream, readErr := c.StreamFromFile(entry.Name())
		if readErr != nil {
			c.logger.ErrorContext(ctx, "Could not read stream file",
				slog.String("file", entry.Name()), slog.Any("error", readErr))
			unreadable[name] = struct{}{}
			continue
		}
		stream.Stream.Name = name
		files[name] = stream
	}
	return files, unreadable, nil
}

// apply creates or updates the stream from its file, unless it is unchanged.
func (c *ConfigSync) apply(ctx context.Context, name string, file Stream) {
	configured, err := c.core.GetStream(ctx, name)
	if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
		c.logger.ErrorContext(ctx, "Could not get stream", slog.String("stream", name), slog.Any("error", err))
		return
	}
	if configured != nil && file.Equals(StreamFromDomain(*configured)) {
		return
	}

	c.logger.DebugContext(ctx, "Upserting stream", slog.String("stream", name))
	if _, err = c.core.UpsertStream(ctx, *file.AsDomain()); err != nil {
		c.logger.ErrorContext(ctx, "Could not upsert stream", slog.String("stream", name), slog.Any("error", err))
	}
}

func (c *ConfigSync) onCoreChange(event domain.StreamSpecWatchEvent) error {
	path := filepath.Join(c.dir, event.StreamSpec.Name+fileExtension)

	switch event.EventType {
	case domain.StreamSpecWatchEventTypeDelete:
		err := os.Remove(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err

	case domain.StreamSpecWatchEventTypeUpsert:
		stored, err := c.StreamFromFile(event.StreamSpec.Name + fileExtension)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}

		stream := StreamFromDomain(event.StreamSpec)
		if stored.Equals(stream) {
			return nil
		}

		content, err := json.MarshalIndent(stream, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(path, content, filePermissions)
	}
	return nil
}

func (c *ConfigSync) StreamFromFile(fileName string) (Stream, error) {
	content, err := os.ReadFile(filepath.Join(c.dir, fileName))
	if err != nil {
		return Stream{}, err
	}

	var stream Stream
	if err = json.Unmarshal(content, &stream); err != nil {
		return Stream{}, fmt.Errorf("failed to parse %s: %w", fileName, err)
	}
	return stream, nil
}

func (c *ConfigSync) Close() error {
	if c.watcher == nil {
		return nil
	}
	return c.watcher.Close()
}
