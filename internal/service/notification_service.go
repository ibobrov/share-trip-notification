package service

import (
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NotificationService struct {
	pool                *pgxpool.Pool
	notificationUseCase *domain.NotificationUseCase
}

func NewNotificationService(
	pool *pgxpool.Pool,
	tripUseCase *domain.NotificationUseCase,
) *NotificationService {
	return &NotificationService{
		pool:                pool,
		notificationUseCase: tripUseCase,
	}
}
