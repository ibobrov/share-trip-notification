package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
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
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found, using environment variables")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbConfig := repository.Config{
		Host:     config.Env("DB_HOST", "localhost"),
		Port:     config.EnvInt("POSTGRES_PORT", 6544),
		User:     config.Env("POSTGRES_USER", "postgres"),
		Password: config.Env("POSTGRES_PASSWORD", "password"),
		DBName:   config.Env("POSTGRES_DB", "sharetrip-notification"),
		SSLMode:  config.Env("DB_SSLMODE", "disable"),
	}

	pool, err := repository.NewPool(ctx, dbConfig.DSN())
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

	logger, logFile, err := app.NewLogger()
	if err != nil {
		return fmt.Errorf("app.NewLogger: %w", err)
	}
	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			log.Printf("logFile.Close: %v", err)
		}
	}(logFile)
	application.Use(middleware.Correlation(logger))

	server.Route(application.Group(""))
	brokers := strings.Split(config.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	consumer := events.NewConsumer(brokers,
		config.Env("KAFKA_TRIP_TOPIC", "trip.events"),
		config.Env("KAFKA_GROUP_ID", "notification-service"), notificationService)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("close Kafka consumer", "error", err)
		}
	}()
	listener, err := net.Listen("tcp", ":"+config.Env("HTTP_PORT", "4001"))
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
