package api_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/api"
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/stretchr/testify/require"
)

type getNotificationTestResponse struct {
	Data   *api.GetNotificationResponse `json:"data"`
	Errors []string                     `json:"errors"`
}

func sendGetNotificationRequest(t *testing.T, expectedStatus int, notificationID string) getNotificationTestResponse {
	t.Helper()
	return sendJSONRequest[getNotificationTestResponse](t, http.MethodGet, notificationTestURL+notificationID, nil, expectedStatus)
}

func TestServer_GetNotification(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"message":"test"}`)
	created := sendCreateNotificationRequest(t, fiber.StatusCreated, api.CreateNotificationRequest{
		RecipientID: uuid.NewString(),
		Type:        "trip_published",
		Payload:     payload,
	})
	require.Empty(t, created.Errors)
	require.NotNil(t, created.Data)

	response := sendGetNotificationRequest(t, fiber.StatusOK, created.Data.ID.String())
	require.Empty(t, response.Errors)
	require.NotNil(t, response.Data)
	require.Equal(t, created.Data.ID, response.Data.ID)
	require.Equal(t, created.Data.RecipientID, response.Data.RecipientID)
	require.Equal(t, created.Data.Type, response.Data.Type)
	require.Equal(t, created.Data.Status, response.Data.Status)
	require.JSONEq(t, string(payload), string(response.Data.Payload))
	require.WithinDuration(t, created.Data.CreatedAt, response.Data.CreatedAt, time.Microsecond)
}

func TestServer_GetNotification_NotFound(t *testing.T) {
	t.Parallel()

	response := sendGetNotificationRequest(t, fiber.StatusNotFound, uuid.NewString())
	require.Nil(t, response.Data)
	require.NotEmpty(t, response.Errors)
	require.Contains(t, strings.Join(response.Errors, "; "), domain.ErrNotFound.Error())
}

func TestServer_GetNotification_Validation(t *testing.T) {
	t.Parallel()

	response := sendGetNotificationRequest(t, fiber.StatusBadRequest, "not-a-uuid")
	require.Nil(t, response.Data)
	require.Equal(t, []string{"invalid notification id"}, response.Errors)
}
