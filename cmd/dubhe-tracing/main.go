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

	_ "github.com/go-sql-driver/mysql"
	"github.com/zhaochy1990/dubhe/internal/tracing"
	tracingschema "github.com/zhaochy1990/dubhe/src/dubhe"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func openMySQL(dsn, name string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	return db, nil
}

func run() error {
	dsn := os.Getenv("DUBHE_MYSQL_DSN")
	if dsn == "" {
		return errors.New("DUBHE_MYSQL_DSN is required")
	}
	db, err := openMySQL(dsn, "DUBHE_MYSQL_DSN")
	if err != nil {
		return err
	}
	defer db.Close()
	readDB := db
	if readDSN := os.Getenv("DUBHE_MYSQL_READ_DSN"); readDSN != "" {
		readDB, err = openMySQL(readDSN, "DUBHE_MYSQL_READ_DSN")
		if err != nil {
			return err
		}
		defer readDB.Close()
	}
	handler, err := tracing.NewHandler(db, readDB, os.Getenv("DUBHE_TRACING_AUTH"))
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
