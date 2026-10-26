package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/repository/entity"
	"github.com/jackc/pgx/v5"
)

type CreateNotificationRequest struct {
	RecipientID string
	Type        string
	Payload     json.RawMessage
}

type CreateNotificationResponse struct {
	ID          uuid.UUID
	RecipientID string
	Type        string
	Status      string
	Payload     json.RawMessage
	CreatedAt   time.Time
}

func (u *NotificationUseCase) CreateNotification(
	ctx context.Context,
	tx pgx.Tx,
	req CreateNotificationRequest,
) (*CreateNotificationResponse, error) {

	notification := entity.Notification{
		ID:          uuid.New(),
		RecipientID: req.RecipientID,
		Type:        req.Type,
		Status:      string(NotificationStatusCreated),
		Payload:     req.Payload,
		CreatedAt:   time.Now(),
	}

	if _, err := u.notificationRepo.Create(ctx, tx, notification); err != nil {
		return nil, fmt.Errorf("notificationRepo.CreateNotification: %w", err)
	}

	return &CreateNotificationResponse{
		ID:          notification.ID,
		RecipientID: notification.RecipientID,
		Type:        notification.Type,
		Status:      notification.Status,
		Payload:     notification.Payload,
		CreatedAt:   notification.CreatedAt,
	}, nil
}
