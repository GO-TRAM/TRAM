package main

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := newRootCmd(logger).Execute(); err != nil {
		logger.Error("Tram stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

func newRootCmd(logger *slog.Logger) *cobra.Command {
	root := &cobra.Command{
		Use:   "tram",
		Short: "Tram is a simple observability pipeline tool",
		Long: `Tram is a simple observability pipeline tool that allows you to
collect, process, and forward logs, metrics, and traces to various
backends.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.AddCommand(newServerCmd(logger))
	return root
}
