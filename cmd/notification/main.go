package main

import (
	"context"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/api"
	"github.com/ibobrov/share-trip-notification/internal/app"
	"github.com/ibobrov/share-trip-notification/internal/config"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/observability/middleware"
	"github.com/ibobrov/share-trip-notification/internal/repository"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found, using environment variables")
	}

	ctx := context.Background()

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
		log.Fatalf("repository.NewPool: %v", err)
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
		panic(err)
	}
	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			panic(err)
		}
	}(logFile)
	application.Use(middleware.Correlation(logger))

	server.Route(application.Group(""))
	httpPort := config.Env("HTTP_PORT", "8080")
	if err := application.Listen(":" + httpPort); err != nil {
		log.Fatalf("application.Listen: %v", err)
		return
	}
}
