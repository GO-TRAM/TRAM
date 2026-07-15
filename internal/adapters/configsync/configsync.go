package configsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/core"
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

func New(c *core.Core, dir string) (*ConfigSync, error) {
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
	log.Printf("Starting configuration sync on directory: %q", c.dir)

	go func() {
		for event := range c.watcher.Events {
			c.fsEvents <- event
		}
	}()

	ch := make(chan domain.StreamSpecWatchEvent)
	c.core.Watch(ctx, ch)
	defer close(ch)

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-c.watcher.Errors:
			return fmt.Errorf("watcher error: %w", err)

		case event := <-ch:
			if err := c.onCoreChange(event); err != nil {
				log.Printf("Error during core change: %v", err)
			}

		case event := <-c.fsEvents:
			if c.debouncer != nil {
				c.debouncer.Stop()
			}
			if filepath.Ext(event.Name) == ".json" {
				c.debouncer = time.AfterFunc(c.debounceDuration, func() {
					if err := c.sync(); err != nil {
						log.Printf("Error during file sync: %v", err)
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
		log.Printf("Upserting stream %q", spec.Stream.Name)
		configured, err := c.core.GetStream(context.Background(), spec.Stream.Name)
		if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
			log.Printf("Error getting stream %q: %v", spec.Stream.Name, err)
			continue
		}

		if configured != nil && spec.Equals(StreamFromDomain(*configured)) {
			continue
		}

		if _, err := c.core.UpsertStream(context.Background(), *spec.AsDomain()); err != nil {
			log.Printf("Error upserting stream %q: %v", spec.Stream.Name, err)
		}
	}

	for name := range toDelete {
		log.Printf("Deleting stream %q", name)
		if err := c.core.DeleteStream(context.Background(), name); err != nil {
			log.Printf("Error deleting stream %q: %v", name, err)
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
		return Stream{}, err
	}

	var stream Stream
	if err := json.Unmarshal(content, &stream); err != nil {
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
