package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/observability/logctx"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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

func (s *NotificationService) HandleTripPublished(ctx context.Context, req HandleTripPublishedRequest) (response HandleTripPublishedResponse, err error) {
	ctx, span := otel.Tracer("notification").Start(ctx, "PersistNotification")
	defer span.End()
	started := time.Now()
	defer func() {
		result := "created"
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, "operation failed")
		} else if response.AlreadyProcessed {
			s.metrics.InboxDuplicate.Inc()
			return
		}
		s.metrics.PersistTotal.WithLabelValues(result).Inc()
		s.metrics.PersistDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
		logctx.Logger(ctx).Debug("notification persistence completed", "component", "NotificationService", "operation", "PersistNotification", "notification_id", response.NotificationID, "result", result)
	}()
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
