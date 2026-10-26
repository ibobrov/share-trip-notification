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

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/api"
	"github.com/ibobrov/share-trip-notification/internal/app"
	"github.com/ibobrov/share-trip-notification/internal/config"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/events"
	"github.com/ibobrov/share-trip-notification/internal/observability/metrics"
	"github.com/ibobrov/share-trip-notification/internal/observability/middleware"
	"github.com/ibobrov/share-trip-notification/internal/observability/tracing"
	"github.com/ibobrov/share-trip-notification/internal/repository"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	tp, err := tracing.NewProvider(ctx, tracing.Config{ServiceName: "notification", ServiceVersion: "1.0.0", Environment: cfg.AppEnv, Endpoint: cfg.OTLPEndpoint})
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdown); err != nil {
			log.Printf("tracing shutdown: %v", err)
		}
	}()
	registry := prometheus.NewRegistry()
	processMetrics := metrics.New(registry)
	pool, err := repository.NewPool(ctx, cfg.DatabaseDSN)
	if err != nil {
		return fmt.Errorf("repository.NewPool: %w", err)
	}
	defer pool.Close()

	notificationRepo := repository.NewNotificationRepository()
	notificationUseCase := domain.NewNotificationUseCase(notificationRepo)
	notificationService := service.NewNotificationService(pool, notificationUseCase, processMetrics)
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
	application.Use(tracing.NewFiberMiddleware())
	application.Use(middleware.Correlation(logger))
	application.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))

	server.Route(application.Group(""), cfg.RequestTimeout)
	consumer := events.NewConsumer(cfg.KafkaBrokers, cfg.TripEventsTopic, cfg.KafkaGroupID, notificationService, cfg.HandleTimeout, logger, processMetrics)
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
