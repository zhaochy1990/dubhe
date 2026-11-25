package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zhaochy1990/dubhe/internal/tracing"
	tracingschema "github.com/zhaochy1990/dubhe/src/dubhe"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("DUBHE_MYSQL_DSN")
	if dsn == "" {
		return errors.New("DUBHE_MYSQL_DSN is required")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return errors.New("DUBHE_MYSQL_DSN is invalid")
	}
	connector, err := mysql.NewConnector(config)
	if err != nil {
		return errors.New("DUBHE_MYSQL_DSN is invalid")
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	handler, err := tracing.NewHandler(db, os.Getenv("DUBHE_TRACING_AUTH"))
	if err != nil {
		return err
	}

	startup, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	err = tracingschema.Initialize(startup, db)
	cancel()
	if err != nil {
		return fmt.Errorf("initialize tracing schema: %w", err)
	}
	address := os.Getenv("DUBHE_LISTEN")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       1 * time.Minute,
	}
	exit, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-exit.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	log.Printf("listening on %s", listener.Addr())
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-done
	return nil
}
