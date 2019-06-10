package pgutil

type Config struct {
	DatabaseURL   string `env:"database_url" required:"true"`
	LogSQLQueries bool   `env:"log_sql_queries" default:"false"`
}
