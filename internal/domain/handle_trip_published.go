package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type HandleTripPublishedRequest struct {
	EventID    uuid.UUID
	TripID     uuid.UUID
	ClientID   uuid.UUID
	CompanyID  string
	OccurredAt time.Time
}

type HandleTripPublishedResponse struct {
	NotificationID   uuid.UUID
	AlreadyProcessed bool
}

func (u *NotificationUseCase) HandleTripPublished(ctx context.Context, tx pgx.Tx, req HandleTripPublishedRequest) (*HandleTripPublishedResponse, error) {
	inserted, err := u.notificationRepo.TryMarkEventProcessed(ctx, tx, req.EventID, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("notificationRepo.TryMarkEventProcessed: %w", err)
	}
	if !inserted {
		return &HandleTripPublishedResponse{AlreadyProcessed: true}, nil
	}
	payload, err := json.Marshal(struct {
		TripID     uuid.UUID `json:"trip_id"`
		CompanyID  string    `json:"company_id"`
		OccurredAt time.Time `json:"occurred_at"`
	}{TripID: req.TripID, CompanyID: req.CompanyID, OccurredAt: req.OccurredAt.UTC()})
	if err != nil {
		return nil, fmt.Errorf("marshal trip publication notification: %w", err)
	}
	notification, err := u.CreateNotification(ctx, tx, CreateNotificationRequest{
		RecipientID: req.ClientID.String(), Type: "trip_published", Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("create trip publication notification: %w", err)
	}
	return &HandleTripPublishedResponse{NotificationID: notification.ID}, nil
}
