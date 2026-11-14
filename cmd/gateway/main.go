package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"local.dev/llm-gateway/internal/gateway"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := gateway.FromEnv()
	if err != nil {
		logger.Error("configuration invalid", "error", err)
		os.Exit(1)
	}
	store, err := gateway.OpenStore(config)
	if err != nil {
		logger.Error("database initialization failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err = store.Migrate(ctx)
	cancel()
	if err != nil {
		logger.Error("schema initialization failed", "error", err)
		os.Exit(1)
	}
	app := gateway.NewServer(config, store, logger)
	defer app.Redis.Close()
	background, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	go app.Maintain(background)
	server := &http.Server{Addr: config.Address, Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", "address", config.Address, "owner_id", store.OwnerID)
		errCh <- server.ListenAndServe()
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	select {
	case err = <-errCh:
		if err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	case <-signals:
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err = server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		cancel()
		logger.Info("gateway shutdown completed")
	}
}
