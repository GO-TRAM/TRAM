package main

import (
	"log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "Stream is a simple observability pipeline tool",
	Long: `Stream is a simple observability pipeline tool that allows you to
	collect, process, and forward logs, metrics, and traces to various
	backends.`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
