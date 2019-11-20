package pgutil

import "github.com/golang-migrate/migrate/v4/source"

type (
	options struct {
		sourceDriver source.Driver
	}

	// ConfigFunc is a function used to configure an initializer.
	ConfigFunc func(*options)
)

// WithMigrationSourceDriver sets the migration source driver.
func WithMigrationSourceDriver(sourceDriver source.Driver) ConfigFunc {
	return func(o *options) { o.sourceDriver = sourceDriver }
}

func getOptions(configs []ConfigFunc) *options {
	options := &options{}
	for _, f := range configs {
		f(options)
	}

	return options
}
