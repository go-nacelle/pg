package main

import (
	"os"

	"github.com/go-nacelle/pgutil/cmd/migrate/internal/commands"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/logging"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage and execute Postgres schema migrations",
}

func init() {
	logger, err := logging.CreateLogger()
	if err != nil {
		panic(err)
	}

	rootCmd.AddCommand(commands.CreateCommand(logger))
	rootCmd.AddCommand(commands.UpCommand(logger))
	rootCmd.AddCommand(commands.UndoCommand(logger))
	rootCmd.AddCommand(commands.StatCommand(logger))
	rootCmd.AddCommand(commands.WriteMigrationLogCommand(logger))
	rootCmd.AddCommand(commands.DescribeCommand(logger))
	rootCmd.AddCommand(commands.DriftCommand(logger))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
