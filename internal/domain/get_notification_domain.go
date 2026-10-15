package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type GetNotificationRequest struct {
	NotificationID uuid.UUID
}

type GetNotificationResponse struct {
	ID          uuid.UUID
	RecipientID string
	Type        string
	Status      string
	Payload     json.RawMessage
	CreatedAt   time.Time
}

func (u *NotificationUseCase) GetNotification(
	ctx context.Context,
	tx pgx.Tx,
	req GetNotificationRequest,
) (*GetNotificationResponse, error) {
	notification, err := u.notificationRepo.GetNotification(ctx, tx, req.NotificationID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}

		return nil, fmt.Errorf("get notification: %w", err)
	}

	return &GetNotificationResponse{
		ID:          notification.ID,
		RecipientID: notification.RecipientID,
		Type:        notification.Type,
		Status:      notification.Status,
		Payload:     notification.Payload,
		CreatedAt:   notification.CreatedAt,
	}, nil
}
