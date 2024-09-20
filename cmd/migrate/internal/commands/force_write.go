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

func WriteMigrationLogCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL         string
		migrationsDirectory string
	)

	writeMigrationLogCmd := &cobra.Command{
		Use:   "write-migration-log <migration-id>",
		Short: "Write a successful migration log entry without running a migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			migrationID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid migration ID: %v", err)
			}

			return writeMigrationLog(databaseURL, migrationsDirectory, logger, migrationID)
		},
	}

	flags.RegisterDatabaseURLFlag(writeMigrationLogCmd, &databaseURL)
	flags.RegisterMigrationsDirectoryFlag(writeMigrationLogCmd, &migrationsDirectory)
	return writeMigrationLogCmd
}

func writeMigrationLog(databaseURL string, migrationsDirectory string, logger log.Logger, migrationID int) error {
	runner, err := database.CreateRunner(databaseURL, migrationsDirectory, logger)
	if err != nil {
		return err
	}

	return runner.WriteMigrationLog(context.Background(), migrationID)
}
