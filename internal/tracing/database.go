package tracing

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"strings"

	tracingschema "github.com/zhaochy1990/dubhe/src/dubhe"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

//go:embed sqlite.sql
var sqliteSchema string

func OpenDatabase(ctx context.Context, driver, dsn string) (*gorm.DB, error) {
	if dsn == "" {
		return nil, errors.New("database DSN is required")
	}
	var dialect gorm.Dialector
	switch driver {
	case "mysql":
		dialect = mysql.New(mysql.Config{DSN: dsn, SkipInitializeWithVersion: true})
	case "sqlite":
		dsn, err := sqliteDSN(dsn)
		if err != nil {
			return nil, err
		}
		dialect = sqlite.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported database driver %q", driver)
	}
	db, err := gorm.Open(dialect, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, errors.New("could not open database")
	}
	pool, err := db.DB()
	if err != nil {
		return nil, errors.New("could not get database connection pool")
	}
	if driver == "sqlite" {
		// Serialize local SQLite transactions on each pool. Foreign-key enforcement
		// and the busy timeout are connection settings configured in the DSN.
		pool.SetMaxOpenConns(1)
	}
	if err := pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, errors.New("could not connect to database")
	}
	return db, nil
}

func sqliteDSN(dsn string) (string, error) {
	path, options, _ := strings.Cut(dsn, "?")
	params, err := url.ParseQuery(options)
	if path == "" || err != nil {
		return "", errors.New("invalid SQLite DSN")
	}
	params.Set("_foreign_keys", "on")
	params.Del("_fk")
	if !params.Has("_busy_timeout") && !params.Has("_timeout") {
		params.Set("_busy_timeout", "5000")
	}
	return path + "?" + params.Encode(), nil
}

func InitializeDatabase(ctx context.Context, db *gorm.DB) error {
	if db.Dialector.Name() == "mysql" {
		pool, err := db.DB()
		if err != nil {
			return err
		}
		return tracingschema.Initialize(ctx, pool)
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for statement := range strings.SplitSeq(sqliteSchema, ";") {
			if strings.TrimSpace(statement) != "" {
				if err := tx.Exec(statement).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s store) readTransaction(ctx context.Context, query func(*gorm.DB) error) error {
	options := &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
	if s.db.Dialector.Name() == "sqlite" {
		// SQLite holds one consistent snapshot for the lifetime of a read transaction.
		return s.db.WithContext(ctx).Transaction(query)
	}
	return s.db.WithContext(ctx).Transaction(query, options)
}

func lockedRow(tx *gorm.DB, query string, args ...any) *sql.Row {
	if tx.Dialector.Name() == "mysql" {
		query += " FOR UPDATE"
	}
	return tx.Raw(query, args...).Row()
}

func (s store) timestamp(ns uint64) any {
	if s.db.Dialector.Name() == "sqlite" {
		// SQLite integers are signed. Fixed-width decimal text keeps all OTLP
		// nanosecond values exact and sorts in the same order as unsigned integers.
		return fmt.Sprintf("%020d", ns)
	}
	return ns
}
