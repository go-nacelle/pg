package pgutil

type (
	options struct {
		// For expansion
	}

	// ConfigFunc is a function used to configure an initializer.
	ConfigFunc func(*options)
)

func getOptions(configs []ConfigFunc) *options {
	options := &options{}
	for _, f := range configs {
		f(options)
	}

	return options
}
