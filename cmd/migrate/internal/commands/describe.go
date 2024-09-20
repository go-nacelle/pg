package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/database"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/flags"
	"github.com/spf13/cobra"
)

func DescribeCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL string
	)

	describeCmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe the current database schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			return describe(databaseURL, logger)
		},
	}

	flags.RegisterDatabaseURLFlag(describeCmd, &databaseURL)
	return describeCmd
}

func describe(databaseURL string, logger log.Logger) error {
	db, err := database.Dial(databaseURL, logger)
	if err != nil {
		return err
	}

	description, err := pgutil.DescribeSchema(context.Background(), db)
	if err != nil {
		return err
	}

	serialized, err := json.Marshal(description)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", serialized)
	return nil
}
