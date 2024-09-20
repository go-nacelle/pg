package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/go-nacelle/log/v2"
	"github.com/go-nacelle/pgutil"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/database"
	"github.com/go-nacelle/pgutil/cmd/migrate/internal/flags"
	"github.com/spf13/cobra"
)

func StatCommand(logger log.Logger) *cobra.Command {
	var (
		databaseURL         string
		migrationsDirectory string
	)

	stateCmd := &cobra.Command{
		Use:   "state",
		Short: "Display the current state of the database schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			return state(databaseURL, migrationsDirectory, logger)
		},
	}

	flags.RegisterDatabaseURLFlag(stateCmd, &databaseURL)
	flags.RegisterMigrationsDirectoryFlag(stateCmd, &migrationsDirectory)
	flags.RegisterNoColorFlag(stateCmd)
	return stateCmd
}

func state(databaseURL, migrationsDirectory string, logger log.Logger) error {
	runner, err := database.CreateRunner(databaseURL, migrationsDirectory, logger)
	if err != nil {
		return err
	}

	definitions := runner.Definitions()

	logs, err := runner.MigrationLogs(context.Background())
	if err != nil {
		return err
	}
	logMap := map[int]pgutil.MigrationLog{}
	for _, log := range logs {
		logMap[log.MigrationID] = log
	}

	maxDefinitionLen := 0
	for _, definition := range definitions {
		if len(definition.Name) > maxDefinitionLen {
			maxDefinitionLen = len(definition.Name)
		}
	}

	type migrationError struct {
		definition   pgutil.Definition
		errorMessage string
	}
	errorMessages := []migrationError{}

	for _, definition := range definitions {
		log, exists := logMap[definition.ID]
		color, statusEmoji, statusText := definitionStatus(log, exists)

		color.Printf(
			"%s %04d: %s\t\t%s\n",
			statusEmoji,
			definition.ID,
			definition.Name+strings.Repeat(" ", maxDefinitionLen-len(definition.Name)),
			statusText,
		)

		if exists && log.ErrorMessage != nil {
			errorMessages = append(errorMessages, migrationError{
				definition:   definition,
				errorMessage: *log.ErrorMessage,
			})
		}
	}

	if len(errorMessages) > 0 {
		fmt.Println("\nErrors:")

		for _, message := range errorMessages {
			fmt.Printf("  %04d: %s\n", message.definition.ID, message.errorMessage)
		}
	}

	return nil
}

const (
	emojiApplied    = "✅"
	emojiError      = "❌"
	emojiUnknown    = "❓"
	emojiReverse    = "↩️"
	emojiNotApplied = "  "
)

func definitionStatus(log pgutil.MigrationLog, exists bool) (_ *color.Color, statusEmoji string, statusText string) {
	if !exists {
		return color.New(color.FgCyan), emojiNotApplied, "Not applied"
	}

	if log.Success != nil {
		if *log.Success {
			if !log.Reverse {
				return color.New(color.FgGreen), emojiApplied, "Successfully applied"
			} else {
				return color.New(color.FgYellow), emojiReverse, "Successfully un-apply"
			}
		} else {
			if !log.Reverse {
				return color.New(color.FgRed), emojiError, "Failed most recent apply"
			} else {
				return color.New(color.FgRed), emojiError, "Failed most recent un-apply"
			}
		}
	}

	if !log.Reverse {
		return color.New(color.FgMagenta), emojiUnknown, "Attempted apply (unknown status)"
	} else {
		return color.New(color.FgMagenta), emojiUnknown, "Attempted un-apply (unknown status)"
	}
}
