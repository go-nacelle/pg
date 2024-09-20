package flags

import (
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

func RegisterNoColorFlag(cmd *cobra.Command) {
	var noColor bool

	cmd.PersistentFlags().BoolVarP(
		&noColor,
		"no-color",
		"",
		false,
		"Disable color output",
	)

	registerPreRun(cmd, func(cmd *cobra.Command, args []string) error {
		if noColor {
			color.NoColor = true
		}

		return nil
	})
}
