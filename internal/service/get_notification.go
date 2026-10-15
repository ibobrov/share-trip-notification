package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/observability/logctx"
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

func (s *NotificationService) GetNotification(ctx context.Context, req GetNotificationRequest) (GetNotificationResponse, error) {
	logger := logctx.Logger(ctx).With(
		slog.String("service", "NotificationService"),
		slog.String("operation", "GetNotification"),
		slog.String("layer", "service"),
	)

	result, err := tx(
		ctx,
		s.pool,
		func(tx pgx.Tx) (*GetNotificationResponse, error) {
			res, err := s.notificationUseCase.GetNotification(ctx, tx, domain.GetNotificationRequest{NotificationID: req.NotificationID})

			if err != nil {

				logger.Error(
					"get notification usecase failed",
					slog.String("layer", "transaction"),
					slog.Any("error", err),
				)
				return nil, fmt.Errorf("usecase.GetNotification: %w", err)
			}

			return &GetNotificationResponse{
				ID:          res.ID,
				RecipientID: res.RecipientID,
				Type:        res.Type,
				Status:      res.Status,
				Payload:     res.Payload,
				CreatedAt:   res.CreatedAt,
			}, nil
		},
	)

	if err != nil {
		return GetNotificationResponse{}, err
	}

	return GetNotificationResponse{
		ID:          result.ID,
		RecipientID: result.RecipientID,
		Type:        result.Type,
		Status:      result.Status,
		Payload:     result.Payload,
		CreatedAt:   result.CreatedAt,
	}, nil
}
