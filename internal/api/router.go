package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/observability/middleware"
)

func (s *Server) Route(route fiber.Router, requestTimeout time.Duration) {
	notifications := route.Group(
		"/api/notification",
		middleware.TimeoutMiddleware(requestTimeout),
	)

	notifications.Get("/:notificationId", s.GetNotification)
	notifications.Post("/", s.CreateNotification)
}
