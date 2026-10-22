package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/domain"
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

type TripPublishedHandler interface {
	HandleTripPublished(context.Context, HandleTripPublishedRequest) (HandleTripPublishedResponse, error)
}

func (s *NotificationService) HandleTripPublished(ctx context.Context, req HandleTripPublishedRequest) (HandleTripPublishedResponse, error) {
	result, err := tx(ctx, s.pool, func(tx pgx.Tx) (*HandleTripPublishedResponse, error) {
		response, err := s.notificationUseCase.HandleTripPublished(ctx, tx, domain.HandleTripPublishedRequest{
			EventID: req.EventID, TripID: req.TripID, ClientID: req.ClientID,
			CompanyID: req.CompanyID, OccurredAt: req.OccurredAt,
		})
		if err != nil {
			return nil, fmt.Errorf("notificationUseCase.HandleTripPublished: %w", err)
		}
		return &HandleTripPublishedResponse{
			NotificationID: response.NotificationID, AlreadyProcessed: response.AlreadyProcessed,
		}, nil
	})
	if err != nil {
		return HandleTripPublishedResponse{}, fmt.Errorf("handle TripPublished transaction: %w", err)
	}
	return *result, nil
}
