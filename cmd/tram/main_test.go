package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogLevelFlag(t *testing.T) {
	t.Parallel()

	run := func(args ...string) (*slog.LevelVar, error) {
		level := &slog.LevelVar{}
		cmd := newRootCmd(slog.New(slog.DiscardHandler), level)
		cmd.SetArgs(args)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		return level, cmd.Execute()
	}

	level, err := run()
	require.NoError(t, err)
	assert.Equal(t, slog.LevelInfo, level.Level(), "info by default")

	level, err = run("--log-level", "debug")
	require.NoError(t, err)
	assert.Equal(t, slog.LevelDebug, level.Level())

	_, err = run("--log-level", "loud")
	assert.ErrorContains(t, err, "invalid --log-level")
}
