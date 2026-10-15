package domain

import "github.com/ibobrov/share-trip-notification/internal/repository"

type NotificationStatus string

const (
	NotificationStatusCreated NotificationStatus = "created"
)

type NotificationUseCase struct {
	notificationRepo *repository.NotificationRepository
}

func NewNotificationUseCase(
	notificationRepo *repository.NotificationRepository,
) *NotificationUseCase {
	return &NotificationUseCase{
		notificationRepo: notificationRepo,
	}
}
