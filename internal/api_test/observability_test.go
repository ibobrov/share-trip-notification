package api_test

import (
	"github.com/ibobrov/share-trip-notification/internal/domain"
	"github.com/ibobrov/share-trip-notification/internal/observability/metrics"
	"github.com/ibobrov/share-trip-notification/internal/repository"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNotificationService_MetricsFollowCommittedOutcome(t *testing.T) {
	t.Parallel()
	observer := metrics.New(prometheus.NewRegistry())
	useCase := domain.NewNotificationUseCase(repository.NewNotificationRepository())
	notificationService := service.NewNotificationService(testPool, useCase, observer)
	request := newTripPublishedRequest()
	_, err := notificationService.HandleTripPublished(t.Context(), request)
	require.NoError(t, err)
	duplicate, err := notificationService.HandleTripPublished(t.Context(), request)
	require.NoError(t, err)
	require.True(t, duplicate.AlreadyProcessed)
	requireEventCounts(t, request, 1, 1)
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.PersistTotal.WithLabelValues("created")), 1e-9)
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.InboxDuplicate), 1e-9)
	failed := newTripPublishedRequest()
	failed.OccurredAt = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	_, err = notificationService.HandleTripPublished(t.Context(), failed)
	require.Error(t, err)
	requireEventCounts(t, failed, 0, 0)
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.PersistTotal.WithLabelValues("error")), 1e-9)
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.PersistTotal.WithLabelValues("created")), 1e-9)
}
