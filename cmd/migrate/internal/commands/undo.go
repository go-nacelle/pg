package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/database"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/flags"
	"github.com/spf13/cobra"
)

func UndoCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL         string
		migrationsDirectory string
	)

	undoCmd := &cobra.Command{
		Use:   "undo <migration-id>",
		Short: "Undo migrations up to and including the specified migration ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			migrationID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid migration ID: %v", err)
			}

			return undo(databaseURL, migrationsDirectory, logger, migrationID)
		},
	}

	flags.RegisterDatabaseURLFlag(undoCmd, &databaseURL)
	flags.RegisterMigrationsDirectoryFlag(undoCmd, &migrationsDirectory)
	return undoCmd
}

func undo(databaseURL string, migrationsDirectory string, logger log.Logger, migrationID int) error {
	runner, err := database.CreateRunner(databaseURL, migrationsDirectory, logger)
	if err != nil {
		return err
	}

	return runner.Undo(context.Background(), migrationID)
}
