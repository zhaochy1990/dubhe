package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLConfigAndSelectDatabase(t *testing.T) {
	for _, name := range []string{"DUBHE_DATABASE_DRIVER", "DUBHE_LISTEN", "DUBHE_MYSQL_DSN", "DUBHE_MYSQL_READ_DSN",
		"DUBHE_SQLITE_DSN", "DUBHE_SQLITE_READ_DSN", "DUBHE_TRACING_AUTH"} {
		t.Setenv(name, "")
	}
	path := filepath.Join(t.TempDir(), "tracing.yaml")
	yaml := []byte(`
server:
  listen: "127.0.0.1:9050"
database:
  driver: sqlite
  sqlite:
    dsn: "testing.db"
  mysql:
    dsn: "mysql-dsn"
    read_dsn: "mysql-read-dsn"
auth:
  readers:
    - token: "Case.Sensitive-Token"
      scope: {tenant_id: "Tenant", environment_id: "staging", project_id: "project"}
      agent_ids: ["agent"]
`)
	if err := os.WriteFile(path, yaml, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Driver != "sqlite" || cfg.Database.SQLite.DSN != "testing.db" || cfg.Server.Listen != "127.0.0.1:9050" {
		t.Fatalf("wrong selected profile or listen address: %+v", cfg.Database)
	}
	if len(cfg.Auth.Readers) != 1 || cfg.Auth.Readers[0].Token != "Case.Sensitive-Token" || cfg.Auth.Readers[0].Scope.TenantID != "Tenant" {
		t.Fatal("YAML failed to preserve credential or scope casing")
	}

	t.Setenv("DUBHE_DATABASE_DRIVER", "mysql")
	t.Setenv("DUBHE_MYSQL_DSN", "environment-dsn")
	cfg, err = loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Driver != "mysql" || cfg.Database.MySQL.DSN != "environment-dsn" ||
		cfg.Database.MySQL.ReadDSN != "mysql-read-dsn" {
		t.Fatal("environment overrides did not select the configured MySQL profile")
	}
	cfg, err = loadConfig([]string{"--config", path, "--database-driver", "sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Driver != "sqlite" {
		t.Fatal("driver flag did not override the environment")
	}
	if _, err := loadConfig([]string{"--config", path, "--database-driver", "unsupported"}); err == nil {
		t.Fatal("expected an error for an unsupported driver")
	}
}
