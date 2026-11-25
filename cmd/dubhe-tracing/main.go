package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/zhaochy1990/dubhe/internal/tracing"
)

func main() {
	if err := run(os.Args[1:]); err != nil && !errors.Is(err, pflag.ErrHelp) {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	profile, err := cfg.databaseProfile()
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	db, err := tracing.OpenDatabase(startup, cfg.Database.Driver, profile.DSN)
	if err != nil {
		return fmt.Errorf("open write database: %w", err)
	}
	pool, err := db.DB()
	if err != nil {
		return err
	}
	defer pool.Close()

	readDB := db
	if profile.ReadDSN != "" {
		readDB, err = tracing.OpenDatabase(startup, cfg.Database.Driver, profile.ReadDSN)
		if err != nil {
			return fmt.Errorf("open read database: %w", err)
		}
		readPool, err := readDB.DB()
		if err != nil {
			return err
		}
		defer readPool.Close()
	}
	handler, err := tracing.NewHandler(db, readDB, cfg.Auth)
	if err != nil {
		return err
	}
	if err := tracing.InitializeDatabase(startup, db); err != nil {
		return fmt.Errorf("initialize tracing schema: %w", err)
	}
	cancel()
	listener, err := net.Listen("tcp", cfg.Server.Listen)
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
