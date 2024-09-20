package flags

import "github.com/spf13/cobra"

func registerPreRun(cmd *cobra.Command, f func(cmd *cobra.Command, args []string) error) {
	previous := cmd.PersistentPreRunE

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if previous != nil {
			if err := previous(cmd, args); err != nil {
				return err
			}
		}

		return f(cmd, args)
	}
}
