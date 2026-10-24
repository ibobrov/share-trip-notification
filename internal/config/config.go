package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	HTTPPort        string
	DatabaseDSN     string
	LogFile         string
	LogLevel        slog.Level
	RequestTimeout  time.Duration
	KafkaBrokers    []string
	TripEventsTopic string
	KafkaGroupID    string
	HandleTimeout   time.Duration
}

// Load validates configuration before any database, HTTP or Kafka connection is opened.
func Load() (Config, error) {
	var cfg Config
	var err error
	port, err := positiveInt("HTTP_PORT", 4001)
	if err != nil {
		return Config{}, err
	}
	if port > 65535 {
		return Config{}, fmt.Errorf("HTTP_PORT must be between 1 and 65535")
	}
	cfg.HTTPPort = strconv.Itoa(port)
	if cfg.DatabaseDSN, err = databaseDSN(6544); err != nil {
		return Config{}, err
	}
	cfg.LogFile = os.Getenv("LOG_FILE")
	if err = cfg.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "INFO"))); err != nil {
		return Config{}, fmt.Errorf("LOG_LEVEL must be DEBUG, INFO, WARN or ERROR")
	}
	if cfg.RequestTimeout, err = milliseconds("REQUEST_TIMEOUT_MS", 2000); err != nil {
		return Config{}, err
	}
	if cfg.KafkaBrokers, err = brokers(); err != nil {
		return Config{}, err
	}
	cfg.TripEventsTopic = env("KAFKA_TRIP_TOPIC", "trip.events")
	if strings.TrimSpace(cfg.TripEventsTopic) == "" {
		return Config{}, fmt.Errorf("KAFKA_TRIP_TOPIC must not be blank")
	}
	cfg.KafkaGroupID = env("KAFKA_GROUP_ID", "notification-service")
	if strings.TrimSpace(cfg.KafkaGroupID) == "" {
		return Config{}, fmt.Errorf("KAFKA_GROUP_ID must not be blank")
	}
	if cfg.HandleTimeout, err = milliseconds("HANDLE_TIMEOUT_MS", 10000); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func required(key string) (string, error) {
	value := os.Getenv(key)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return value, nil
}

func positiveInt(key string, fallback int) (int, error) {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return value, nil
}

func milliseconds(key string, fallback int) (time.Duration, error) {
	value, err := positiveInt(key, fallback)
	if err != nil {
		return 0, err
	}
	if int64(value) > int64((1<<63-1)/time.Millisecond) {
		return 0, fmt.Errorf("%s is too large", key)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func brokers() ([]string, error) {
	value, err := required("KAFKA_BROKERS")
	if err != nil {
		return nil, err
	}
	result := strings.Split(value, ",")
	for i := range result {
		result[i] = strings.TrimSpace(result[i])
		host, port, err := net.SplitHostPort(result[i])
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || host == "" || number < 1 || number > 65535 {
			return nil, fmt.Errorf("KAFKA_BROKERS must contain host:port addresses separated by commas")
		}
	}
	return result, nil
}

func databaseDSN(defaultPort int) (string, error) {
	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		// Existing local Compose environments can still provide separate database variables.
		user, err := required("POSTGRES_USER")
		if err != nil {
			return "", fmt.Errorf("DATABASE_DSN or POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB are required")
		}
		password, err := required("POSTGRES_PASSWORD")
		if err != nil {
			return "", err
		}
		database, err := required("POSTGRES_DB")
		if err != nil {
			return "", err
		}
		port, err := positiveInt("POSTGRES_PORT", defaultPort)
		if err != nil {
			return "", err
		}
		if port > 65535 {
			return "", fmt.Errorf("POSTGRES_PORT must be between 1 and 65535")
		}
		endpoint := url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: net.JoinHostPort(env("DB_HOST", "localhost"), strconv.Itoa(port)), Path: "/" + database}
		query := url.Values{"sslmode": {env("DB_SSLMODE", "disable")}}
		endpoint.RawQuery = query.Encode()
		dsn = endpoint.String()
	}
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil || parsed.ConnConfig.Database == "" {
		// The parser error may contain the DSN and its password.
		return "", fmt.Errorf("DATABASE_DSN must be a valid PostgreSQL connection string with a database name")
	}
	return dsn, nil
}
