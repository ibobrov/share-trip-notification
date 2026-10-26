package service

import (
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/observability/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type NotificationService struct {
	metrics             *metrics.Metrics
	pool                *pgxpool.Pool
	notificationUseCase *domain.NotificationUseCase
}

func NewNotificationService(
	pool *pgxpool.Pool,
	tripUseCase *domain.NotificationUseCase,
	observer *metrics.Metrics,
) *NotificationService {
	if observer == nil {
		observer = metrics.New(prometheus.NewRegistry())
	}
	return &NotificationService{
		pool:                pool,
		metrics:             observer,
		notificationUseCase: tripUseCase,
	}
}
