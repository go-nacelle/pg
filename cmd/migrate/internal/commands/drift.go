package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/database"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/flags"
	"github.com/spf13/cobra"
)

func DriftCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL string
	)

	driftCmd := &cobra.Command{
		Use:   "drift 'description.json'",
		Short: "Compare the current database schema against the expected schema",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return drift(databaseURL, logger, args[0])
		},
	}

	flags.RegisterDatabaseURLFlag(driftCmd, &databaseURL)
	return driftCmd
}

func drift(databaseURL string, logger log.Logger, filename string) error {
	db, err := database.Dial(databaseURL, logger)
	if err != nil {
		return err
	}

	description, err := pgutil.DescribeSchema(context.Background(), db)
	if err != nil {
		return err
	}

	b, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	var expected pgutil.SchemaDescription
	if err := json.Unmarshal(b, &expected); err != nil {
		return err
	}

	statements := pgutil.Compare(expected, description)

	if len(statements) == 0 {
		fmt.Printf("No drift detected\n")
		return nil
	}

	for _, d := range statements {
		fmt.Printf("%s\n\n", d)
	}

	return nil
}
