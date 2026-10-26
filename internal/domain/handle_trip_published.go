package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
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

func (u *NotificationUseCase) HandleTripPublished(ctx context.Context, tx pgx.Tx, req HandleTripPublishedRequest) (*HandleTripPublishedResponse, error) {
	inboxCtx, span := otel.Tracer("notification").Start(ctx, "InboxInsert")
	inserted, err := u.notificationRepo.TryMarkEventProcessed(inboxCtx, tx, req.EventID, time.Now().UTC())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "inbox insert failed")
	}
	span.End()
	if err != nil {
		return nil, fmt.Errorf("notificationRepo.TryMarkEventProcessed: %w", err)
	}
	if !inserted {
		logctx.Logger(ctx).Debug("inbox event already exists", "component", "NotificationUseCase", "operation", "InboxInsert", "result", "duplicate")
		return &HandleTripPublishedResponse{AlreadyProcessed: true}, nil
	}
	logctx.Logger(ctx).Debug("inbox event inserted in transaction", "component", "NotificationUseCase", "operation", "InboxInsert", "result", "pending_commit")
	payload, err := json.Marshal(struct {
		TripID     uuid.UUID `json:"trip_id"`
		CompanyID  string    `json:"company_id"`
		OccurredAt time.Time `json:"occurred_at"`
	}{TripID: req.TripID, CompanyID: req.CompanyID, OccurredAt: req.OccurredAt.UTC()})
	if err != nil {
		return nil, fmt.Errorf("marshal trip publication notification: %w", err)
	}
	notificationCtx, notificationSpan := otel.Tracer("notification").Start(ctx, "CreateNotification")
	notification, err := u.CreateNotification(notificationCtx, tx, CreateNotificationRequest{
		RecipientID: req.ClientID.String(), Type: "trip_published", Payload: payload,
	})
	if err != nil {
		notificationSpan.RecordError(err)
		notificationSpan.SetStatus(codes.Error, "notification persistence failed")
	}
	notificationSpan.End()
	if err != nil {
		return nil, fmt.Errorf("create trip publication notification: %w", err)
	}
	return &HandleTripPublishedResponse{NotificationID: notification.ID}, nil
}
