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

type CreateNotificationRequest struct {
	RecipientID string          `json:"recipient_id" valid:"required"`
	Type        string          `json:"type" valid:"required"`
	Payload     json.RawMessage `json:"payload" valid:"-"`
}

type CreateNotificationResponse struct {
	ID          uuid.UUID       `json:"id"`
	RecipientID string          `json:"recipient_id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

func (s *Server) CreateNotification(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger := logctx.Logger(ctx).With(
		slog.String("component", "NotificationServer"),
		slog.String("operation", "CreateNotification"),
	)

	var req CreateNotificationRequest
	ok, err := ParseAndValidateRequest(&req, c, logger)
	if !ok {
		return err
	}
	logger.Debug("create notification request accepted")

	res, err := s.notificationService.CreateNotification(c.UserContext(), service.CreateNotificationRequest{
		RecipientID: req.RecipientID,
		Type:        req.Type,
		Payload:     req.Payload,
	})

	if err != nil {
		return HandleError(c, logger, err)
	}

	logger.Info("request completed", "result", "success", "notification_id", res.ID)
	return SuccessWithHttpStatus(c, fiber.StatusCreated, CreateNotificationResponse{
		ID:          res.ID,
		RecipientID: res.RecipientID,
		Type:        res.Type,
		Status:      res.Status,
		Payload:     res.Payload,
		CreatedAt:   res.CreatedAt,
	})
}
