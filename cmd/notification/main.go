package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/api"
	"github.com/ibobrov/share-trip-notification/internal/app"
	"github.com/ibobrov/share-trip-notification/internal/config"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/events"
	"github.com/ibobrov/share-trip-notification/internal/observability/middleware"
	"github.com/ibobrov/share-trip-notification/internal/repository"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load .env: invalid or unreadable file")
	}
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool, err := repository.NewPool(ctx, cfg.DatabaseDSN)
	if err != nil {
		return fmt.Errorf("repository.NewPool: %w", err)
	}
	defer pool.Close()

	notificationRepo := repository.NewNotificationRepository()
	notificationUseCase := domain.NewNotificationUseCase(notificationRepo)
	notificationService := service.NewNotificationService(pool, notificationUseCase)
	server := api.NewServer(pool, notificationService)

	application := fiber.New(fiber.Config{
		EnablePrintRoutes: true,
	})

	logger, logFile, err := app.NewLogger(cfg.LogFile, cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("app.NewLogger: %w", err)
	}
	defer func(logFile *os.File) {
		if logFile == nil {
			return
		}
		err := logFile.Close()
		if err != nil {
			log.Printf("logFile.Close: %v", err)
		}
	}(logFile)
	application.Use(middleware.Correlation(logger))

	server.Route(application.Group(""), cfg.RequestTimeout)
	consumer := events.NewConsumer(cfg.KafkaBrokers, cfg.TripEventsTopic, cfg.KafkaGroupID, notificationService, cfg.HandleTimeout)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("close Kafka consumer", "error", err)
		}
	}()
	listener, err := net.Listen("tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return fmt.Errorf("HTTP listen: %w", err)
	}
	defer func() { _ = listener.Close() }()
	results := make(chan error, 2)
	go func() { results <- application.Listener(listener) }()
	go func() { results <- consumer.Run(ctx) }()
	var runError error
	finished := 0
	select {
	case <-ctx.Done():
	case runError = <-results:
		finished = 1
	}
	cancel()
	if err := application.ShutdownWithTimeout(5 * time.Second); err != nil {
		logger.Error("HTTP shutdown", "error", err)
	}
	_ = listener.Close()
	for finished < 2 {
		err := <-results
		if runError == nil && err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			runError = err
		}
		finished++
	}
	if errors.Is(runError, context.Canceled) || errors.Is(runError, net.ErrClosed) {
		return nil
	}
	return runError
}
