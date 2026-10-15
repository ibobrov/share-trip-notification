package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/api"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/stretchr/testify/require"
)

type createNotificationTestResponse struct {
	Data   *api.CreateNotificationResponse `json:"data"`
	Errors []string                        `json:"errors"`
}

func sendCreateNotificationRequest(t *testing.T, expectedStatus int, payload api.CreateNotificationRequest) createNotificationTestResponse {
	t.Helper()
	return sendJSONRequest[createNotificationTestResponse](t, http.MethodPost, notificationTestURL, payload, expectedStatus)
}

func TestServer_CreateNotification(t *testing.T) {
	t.Parallel()

	recipientID := uuid.NewString()
	payload := json.RawMessage(`{"trip_id":"` + uuid.NewString() + `"}`)
	response := sendCreateNotificationRequest(t, fiber.StatusCreated, api.CreateNotificationRequest{
		RecipientID: recipientID,
		Type:        "trip_created",
		Payload:     payload,
	})

	require.Empty(t, response.Errors)
	require.NotNil(t, response.Data)
	require.NotEqual(t, uuid.Nil, response.Data.ID)
	require.Equal(t, recipientID, response.Data.RecipientID)
	require.Equal(t, "trip_created", response.Data.Type)
	require.Equal(t, string(domain.NotificationStatusCreated), response.Data.Status)
	require.JSONEq(t, string(payload), string(response.Data.Payload))
	require.False(t, response.Data.CreatedAt.IsZero())

	var (
		savedRecipientID string
		savedType        string
		savedStatus      string
		savedPayload     []byte
	)
	err := testPool.QueryRow(
		context.Background(),
		`SELECT recipient_id, type, status, payload FROM notifications WHERE id = $1`,
		response.Data.ID,
	).Scan(&savedRecipientID, &savedType, &savedStatus, &savedPayload)
	require.NoError(t, err)
	require.Equal(t, recipientID, savedRecipientID)
	require.Equal(t, "trip_created", savedType)
	require.Equal(t, string(domain.NotificationStatusCreated), savedStatus)
	require.JSONEq(t, string(payload), string(savedPayload))
}

func TestServer_CreateNotification_Validation(t *testing.T) {
	t.Parallel()

	response := sendCreateNotificationRequest(t, fiber.StatusOK, api.CreateNotificationRequest{
		Payload: json.RawMessage(`{}`),
	})

	require.Nil(t, response.Data)
	require.NotEmpty(t, response.Errors)
}
