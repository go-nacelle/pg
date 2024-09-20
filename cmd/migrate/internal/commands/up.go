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

func UpCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL         string
		migrationsDirectory string
	)

	upCmd := &cobra.Command{
		Use:   "up [migration_id]",
		Short: "Run migrations up to and including the specified migration ID",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var migrationID *int
			if len(args) != 0 {
				val, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid migration ID: %v", err)
				}
				migrationID = &val
			}

			return up(databaseURL, migrationsDirectory, logger, migrationID)
		},
	}

	flags.RegisterDatabaseURLFlag(upCmd, &databaseURL)
	flags.RegisterMigrationsDirectoryFlag(upCmd, &migrationsDirectory)
	return upCmd
}

func up(databaseURL, migrationsDirectory string, logger log.Logger, migrationID *int) error {
	runner, err := database.CreateRunner(databaseURL, migrationsDirectory, logger)
	if err != nil {
		return err
	}

	if migrationID == nil {
		return runner.ApplyAll(context.Background())
	}

	return runner.Apply(context.Background(), *migrationID)
}
