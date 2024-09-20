package flags

import "github.com/spf13/cobra"

func RegisterMigrationsDirectoryFlag(cmd *cobra.Command, migrationDirectory *string) {
	cmd.PersistentFlags().StringVarP(
		migrationDirectory,
		"dir", "d",
		"migrations",
		"The directory where schema migrations are defined",
	)
}
