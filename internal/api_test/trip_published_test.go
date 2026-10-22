package api_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/stretchr/testify/require"
)

func newTripPublishedRequest() service.HandleTripPublishedRequest {
	return service.HandleTripPublishedRequest{
		EventID: uuid.New(), TripID: uuid.New(), ClientID: uuid.New(), CompanyID: "company-acme",
		OccurredAt: time.Now().UTC().Truncate(time.Second),
	}
}

func requireEventCounts(t *testing.T, request service.HandleTripPublishedRequest, notifications, processed int) {
	t.Helper()
	var actualNotifications, actualProcessed int
	err := testPool.QueryRow(t.Context(), `SELECT
  (SELECT count(*) FROM notifications WHERE recipient_id=$1),
  (SELECT count(*) FROM processed_events WHERE event_id=$2)`,
		request.ClientID.String(), request.EventID).Scan(&actualNotifications, &actualProcessed)
	require.NoError(t, err)
	require.Equal(t, notifications, actualNotifications)
	require.Equal(t, processed, actualProcessed)
}

func TestNotificationService_TripPublished(t *testing.T) {
	t.Parallel()
	request := newTripPublishedRequest()
	response, err := testNotificationService.HandleTripPublished(t.Context(), request)
	require.NoError(t, err)
	require.False(t, response.AlreadyProcessed)
	require.NotEqual(t, uuid.Nil, response.NotificationID)
	var recipient, notificationType, status string
	var payload []byte
	err = testPool.QueryRow(t.Context(), `SELECT recipient_id,type,status,payload FROM notifications WHERE id=$1`, response.NotificationID).
		Scan(&recipient, &notificationType, &status, &payload)
	require.NoError(t, err)
	require.Equal(t, request.ClientID.String(), recipient)
	require.Equal(t, "trip_published", notificationType)
	require.Equal(t, "created", status)
	require.JSONEq(t, `{"trip_id":"`+request.TripID.String()+`","company_id":"company-acme","occurred_at":"`+request.OccurredAt.Format(time.RFC3339)+`"}`, string(payload))
	duplicate, err := testNotificationService.HandleTripPublished(t.Context(), request)
	require.NoError(t, err)
	require.True(t, duplicate.AlreadyProcessed)
	requireEventCounts(t, request, 1, 1)
}

func TestNotificationService_TripPublished_ConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	request := newTripPublishedRequest()
	type result struct {
		response service.HandleTripPublishedResponse
		err      error
	}
	results := make(chan result, 6)
	for i := 0; i < 6; i++ {
		go func() {
			response, err := testNotificationService.HandleTripPublished(t.Context(), request)
			results <- result{response: response, err: err}
		}()
	}
	created := 0
	for i := 0; i < 6; i++ {
		result := <-results
		require.NoError(t, result.err)
		if !result.response.AlreadyProcessed {
			created++
		}
	}
	require.Equal(t, 1, created)
	requireEventCounts(t, request, 1, 1)
}

func TestNotificationService_TripPublished_RollbackAllowsRetry(t *testing.T) {
	t.Parallel()
	request := newTripPublishedRequest()
	// Serialization fails after inserting processed_events. Both writes must roll back.
	request.OccurredAt = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	_, err := testNotificationService.HandleTripPublished(t.Context(), request)
	require.Error(t, err)
	requireEventCounts(t, request, 0, 0)
	request.OccurredAt = time.Now().UTC()
	response, err := testNotificationService.HandleTripPublished(t.Context(), request)
	require.NoError(t, err)
	require.False(t, response.AlreadyProcessed)
	requireEventCounts(t, request, 1, 1)
}
