package configsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
	"github.com/fsnotify/fsnotify"
)

type ConfigSync struct {
	dir              string
	core             ports.Core
	watcher          *fsnotify.Watcher
	fsEvents         chan fsnotify.Event
	debouncer        *time.Timer
	debounceDuration time.Duration
}

func New(c ports.Core, dir string) (*ConfigSync, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("failed to watch directory %q: %w", dir, err)
	}

	cs := &ConfigSync{
		dir:              dir,
		core:             c,
		watcher:          watcher,
		fsEvents:         make(chan fsnotify.Event, 100),
		debounceDuration: 100 * time.Millisecond,
	}

	if err := cs.sync(); err != nil {
		watcher.Close()
		return nil, err
	}

	return cs, nil
}

func (c *ConfigSync) Run(ctx context.Context) error {
	slog.Info("Starting configuration sync", "directory", c.dir)

	go func() {
		for event := range c.watcher.Events {
			slog.Debug("File system event received", "path", event.Name, "op", event.Op)
			c.fsEvents <- event
		}
	}()

	// Cancelling on return unregisters the watch, so the core never blocks
	// on a channel nobody reads anymore.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan domain.StreamSpecWatchEvent)
	if err := c.core.Watch(ctx, ch); err != nil {
		return fmt.Errorf("failed to watch core: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-c.watcher.Errors:
			slog.Error("Watcher error", "error", err)
			return fmt.Errorf("watcher error: %w", err)

		case event := <-ch:
			if err := c.onCoreChange(event); err != nil {
				slog.Error("Error during core change", "error", err)
			}

		case event := <-c.fsEvents:
			if c.debouncer != nil {
				c.debouncer.Stop()
			}
			if filepath.Ext(event.Name) == ".json" {
				c.debouncer = time.AfterFunc(c.debounceDuration, func() {
					if err := c.sync(); err != nil {
						slog.Error("Error during file sync", "error", err)
					}
				})
			}
		}
	}
}

func (c *ConfigSync) sync() error {
	files, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}

	configured, err := c.core.GetStreams(context.Background())
	if err != nil {
		return err
	}

	toDelete := make(map[string]domain.StreamSpec, len(configured))
	for _, stream := range configured {
		toDelete[stream.Name] = stream
	}

	streamSpecs := make([]Stream, 0, len(files))

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		config, err := c.StreamFromFile(file.Name())
		if err != nil {
			continue
		}

		nameWithoutExt := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		config.Stream.Name = nameWithoutExt
		streamSpecs = append(streamSpecs, config)
		delete(toDelete, nameWithoutExt)
	}

	for _, spec := range streamSpecs {
		slog.Debug("Processing stream", "name", spec.Stream.Name)
		configured, err := c.core.GetStream(context.Background(), spec.Stream.Name)
		if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
			slog.Error("Failed to get stream", "name", spec.Stream.Name, "error", err)
			continue
		}

		if configured != nil && spec.Equals(StreamFromDomain(*configured)) {
			slog.Debug("Stream unchanged, skipping", "name", spec.Stream.Name)
			continue
		}

		slog.Debug("Upserting stream", "name", spec.Stream.Name)
		if _, err := c.core.UpsertStream(context.Background(), *spec.AsDomain()); err != nil {
			slog.Error("Failed to upsert stream", "name", spec.Stream.Name, "error", err)
		}
	}

	for name := range toDelete {
		slog.Debug("Deleting stream", "name", name)
		if err := c.core.DeleteStream(context.Background(), name); err != nil {
			slog.Error("Failed to delete stream", "name", name, "error", err)
		}
	}

	return nil
}

func (c *ConfigSync) onCoreChange(event domain.StreamSpecWatchEvent) error {
	switch event.EventType {
	case domain.StreamSpecWatchEventTypeDelete:
		path := filepath.Join(c.dir, event.StreamSpec.Name+".json")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		return os.Remove(path)

	case domain.StreamSpecWatchEventTypeUpsert:
		path := filepath.Join(c.dir, event.StreamSpec.Name+".json")
		stored, err := c.StreamFromFile(event.StreamSpec.Name + ".json")
		if err != nil && !os.IsNotExist(err) {
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
		return os.WriteFile(path, content, 0644)
	}
	return nil
}

func (c ConfigSync) StreamFromFile(fileName string) (Stream, error) {
	filePath := filepath.Join(c.dir, fileName)
	content, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("Failed to read config file", "path", filePath, "error", err)
		return Stream{}, err
	}

	var stream Stream
	if err := json.Unmarshal(content, &stream); err != nil {
		slog.Error("Failed to parse config file", "path", filePath, "error", err)
		return Stream{}, err
	}
	return stream, nil
}

func (c ConfigSync) Close() error {
	if c.watcher != nil {
		if err := c.watcher.Close(); err != nil {
			return err
		}
	}
	return nil
}
