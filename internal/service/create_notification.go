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

func (s *NotificationService) CreateNotification(
	ctx context.Context,
	req CreateNotificationRequest,
) (CreateNotificationResponse, error) {
	logger := logctx.Logger(ctx).With(
		slog.String("service", "NotificationService"),
		slog.String("operation", "CreateNotification"),
		slog.String("layer", "service"),
	)

	result, err := tx(ctx, s.pool, func(tx pgx.Tx) (*CreateNotificationResponse, error) {
		res, err := s.notificationUseCase.CreateNotification(ctx, tx, domain.CreateNotificationRequest{
			RecipientID: req.RecipientID,
			Type:        req.Type,
			Payload:     req.Payload,
		})

		if err != nil {

			logger.Error(
				"create notification usecase failed",
				slog.String("layer", "transaction"),
				slog.Any("error", err),
			)
			return nil, fmt.Errorf("usecase.CreateNotification: %w", err)
		}

		return &CreateNotificationResponse{
			ID:          res.ID,
			RecipientID: res.RecipientID,
			Type:        res.Type,
			Status:      res.Status,
			Payload:     res.Payload,
			CreatedAt:   res.CreatedAt,
		}, nil
	})

	if err != nil {
		logger.Error(
			"create notification failed",
			slog.Any("error", err),
		)
		return CreateNotificationResponse{}, fmt.Errorf("failed in transaction: %w", err)
	}

	return *result, nil
}
