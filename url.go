package pgutil

import (
	"fmt"
	"net/url"
	"os"
)

func BuildDatabaseURL() string {
	var (
		host     = getEnvOrDefault("PGHOST", "localhost")
		port     = getEnvOrDefault("PGPORT", "5432")
		user     = getEnvOrDefault("PGUSER", "")
		password = getEnvOrDefault("PGPASSWORD", "")
		database = getEnvOrDefault("PGDATABASE", "")
		sslmode  = getEnvOrDefault("PGSSLMODE", "disable")
	)

	u := &url.URL{
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%s", host, port),
		User:     url.UserPassword(user, password),
		Path:     database,
		RawQuery: url.Values{"sslmode": []string{sslmode}}.Encode(),
	}
	return (u).String()
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}
