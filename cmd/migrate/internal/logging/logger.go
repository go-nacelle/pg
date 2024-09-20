package logging

import (
	"github.com/go-nacelle/config/v3"
	"github.com/go-nacelle/log/v2"
)

func CreateLogger() (log.Logger, error) {
	cfg := config.NewConfig(config.NewEnvSourcer("PGUTIL"))
	if err := cfg.Init(); err != nil {
		return nil, err
	}

	c := &log.Config{}
	if err := cfg.Load(c); err != nil {
		return nil, err
	}

	return log.InitLogger(c)
}
