package pgutil

type Config struct {
	DatabaseURL                 string `env:"database_url" required:"true"`
	LogSQLQueries               bool   `env:"log_sql_queries" default:"false"`
	MigrationsTable             string `env:"migrations_table"`
	MigrationsSchemaName        string `env:"migrations_schema_name"`
	FailOnNewerMigrationVersion bool   `env:"fail_on_newer_migration_version"`
}
