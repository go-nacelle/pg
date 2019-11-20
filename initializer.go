package pgutil

import (
	"github.com/go-nacelle/nacelle"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/lib/pq"
)

type Initializer struct {
	Logger       nacelle.Logger           `service:"logger"`
	Services     nacelle.ServiceContainer `service:"services"`
	sourceDriver source.Driver
}

const ServiceName = "db"

func NewInitializer(configs ...ConfigFunc) *Initializer {
	options := getOptions(configs)

	return &Initializer{
		sourceDriver: options.sourceDriver,
	}
}

func (i *Initializer) Init(config nacelle.Config) error {
	dbConfig := &Config{}
	if err := config.Load(dbConfig); err != nil {
		return err
	}

	logger := i.Logger
	if !dbConfig.LogSQLQueries {
		logger = nacelle.NewNilLogger()
	}

	db, err := Dial(dbConfig.DatabaseURL, logger)
	if err != nil {
		return err
	}

	if err := runMigrations(
		db.DB.DB,
		i.sourceDriver,
		dbConfig.MigrationsTable,
		dbConfig.MigrationsSchemaName,
		i.Logger,
	); err != nil {
		return err
	}

	return i.Services.Set(ServiceName, db)
}
