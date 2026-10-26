package api

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/observability/logctx"
	"github.com/ibobrov/share-trip-notification/internal/service"
)

type GetNotificationResponse struct {
	ID          uuid.UUID       `json:"id"`
	RecipientID string          `json:"recipient_id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

func (s *Server) GetNotification(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger := logctx.Logger(ctx).With(
		slog.String("component", "NotificationServer"),
		slog.String("operation", "GetNotification"),
	)
	logger.Debug("get notification request accepted")

	notificationID, err := uuid.Parse(c.Params("notificationId"))
	if err != nil {
		return FailureWithHttpStatus(c, fiber.StatusBadRequest, "invalid notification id")
	}

	res, err := s.notificationService.GetNotification(ctx, service.GetNotificationRequest{
		NotificationID: notificationID,
	})

	if err != nil {
		return HandleError(c, logger, err)
	}

	logger.Info("request completed", "result", "success", "notification_id", res.ID)
	return Success(c, GetNotificationResponse{
		ID:          res.ID,
		RecipientID: res.RecipientID,
		Type:        res.Type,
		Status:      res.Status,
		Payload:     res.Payload,
		CreatedAt:   res.CreatedAt,
	})
}
