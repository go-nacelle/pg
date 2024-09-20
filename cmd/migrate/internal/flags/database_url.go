package flags

import (
	"fmt"
	"net/url"

	"github.com/go-nacelle/pgutil"
	"github.com/spf13/cobra"
)

func RegisterDatabaseURLFlag(cmd *cobra.Command, databaseURL *string) {
	defaultURL := pgutil.BuildDatabaseURL()

	masked, err := maskDatabasePassword(defaultURL)
	if err != nil {
		panic(err)
	}

	cmd.PersistentFlags().StringVarP(
		databaseURL,
		"url", "u",
		"",
		fmt.Sprintf("The database connection URL (default %s)", masked),
	)

	registerPreRun(cmd, func(cmd *cobra.Command, args []string) error {
		if *databaseURL == "" {
			*databaseURL = defaultURL
		}

		return nil
	})
}

func maskDatabasePassword(databaseURL string) (string, error) {
	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse database URL: %w", err)
	}

	if parsedURL.User != nil {
		if _, ok := parsedURL.User.Password(); ok {
			parsedURL.User = url.UserPassword(parsedURL.User.Username(), "xxxxx")
		}
	}

	return parsedURL.String(), nil
}
