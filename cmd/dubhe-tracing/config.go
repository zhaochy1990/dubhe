package main

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/zhaochy1990/dubhe/internal/tracing"
)

type databaseConfig struct {
	DSN     string `mapstructure:"dsn"`
	ReadDSN string `mapstructure:"read_dsn"`
}

type config struct {
	Server struct {
		Listen string `mapstructure:"listen"`
	} `mapstructure:"server"`
	Database struct {
		Driver string         `mapstructure:"driver"`
		MySQL  databaseConfig `mapstructure:"mysql"`
		SQLite databaseConfig `mapstructure:"sqlite"`
	} `mapstructure:"database"`
	Tracing    tracing.Options    `mapstructure:"tracing"`
	Auth       tracing.AuthConfig `mapstructure:"auth"`
	LegacyAuth string             `mapstructure:"legacy_auth"`
}

func loadConfig(args []string) (config, error) {
	flags := pflag.NewFlagSet("dubhe-tracing", pflag.ContinueOnError)
	file := flags.String("config", "", "Path to a YAML configuration file")
	flags.String("database-driver", "", "Database driver: mysql or sqlite")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("unexpected positional arguments")
	}

	settings := viper.New()
	settings.SetDefault("server.listen", "127.0.0.1:8080")
	settings.SetDefault("database.driver", "mysql")
	if err := settings.BindPFlag("database.driver", flags.Lookup("database-driver")); err != nil {
		return config{}, err
	}
	for key, env := range map[string]string{
		"server.listen":            "DUBHE_LISTEN",
		"database.driver":          "DUBHE_DATABASE_DRIVER",
		"database.mysql.dsn":       "DUBHE_MYSQL_DSN",
		"database.mysql.read_dsn":  "DUBHE_MYSQL_READ_DSN",
		"database.sqlite.dsn":      "DUBHE_SQLITE_DSN",
		"database.sqlite.read_dsn": "DUBHE_SQLITE_READ_DSN",
		"legacy_auth":              "DUBHE_TRACING_AUTH",
	} {
		if err := settings.BindEnv(key, env); err != nil {
			return config{}, err
		}
	}
	if *file != "" {
		settings.SetConfigFile(*file)
		settings.SetConfigType("yaml")
		if err := settings.ReadInConfig(); err != nil {
			return config{}, fmt.Errorf("read YAML configuration: %w", err)
		}
	}
	if !settings.IsSet("auth") && !settings.IsSet("legacy_auth") {
		return config{}, errors.New("auth configuration is required")
	}

	var cfg config
	if err := settings.UnmarshalExact(&cfg); err != nil {
		// Configuration decoding errors can include credential values.
		return config{}, errors.New("invalid tracing configuration")
	}
	if cfg.LegacyAuth != "" {
		var err error
		cfg.Auth, err = tracing.ParseLegacyAuth(cfg.LegacyAuth)
		if err != nil {
			return config{}, err
		}
	}
	profile, err := cfg.databaseProfile()
	if err != nil {
		return config{}, err
	}
	if profile.DSN == "" {
		return config{}, fmt.Errorf("database.%s.dsn is required", cfg.Database.Driver)
	}
	if cfg.Server.Listen == "" {
		return config{}, errors.New("server.listen is required")
	}
	return cfg, nil
}

func (cfg config) databaseProfile() (databaseConfig, error) {
	switch cfg.Database.Driver {
	case "mysql":
		return cfg.Database.MySQL, nil
	case "sqlite":
		return cfg.Database.SQLite, nil
	default:
		return databaseConfig{}, fmt.Errorf("unsupported database driver %q", cfg.Database.Driver)
	}
}
