package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ibobrov/share-trip-notification/internal/observability/logctx"
	"github.com/ibobrov/share-trip-notification/internal/observability/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestConsumer_MetadataAndLinkedTrace(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, provider.Shutdown(ctx))
	})
	observer := metrics.New(prometheus.NewRegistry())
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	calls := []string{}
	reader := &readerStub{calls: &calls, messages: []kafka.Message{{Value: []byte(validTripPublished), Headers: []kafka.Header{
		{Key: "event_id", Value: []byte("78910abc-1234-4567-89ab-1234567890ab")},
		{Key: "event_type", Value: []byte("TripPublished")},
		{Key: "request_id", Value: []byte("request-123")},
		{Key: "correlation_id", Value: []byte("publication-456")},
		{Key: "causation_id", Value: []byte("command-789")},
		{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
	}}}}
	handler := &handlerStub{calls: &calls}
	consumer := &Consumer{reader: reader, handler: handler, handleTimeout: time.Second, logger: logger, metrics: observer, tracer: provider.Tracer("test")}
	require.ErrorIs(t, consumer.Run(t.Context()), io.EOF)
	require.Len(t, handler.contexts, 1)
	ctx := handler.contexts[0]
	require.Equal(t, "request-123", logctx.RequestID(ctx))
	require.Equal(t, "publication-456", logctx.CorrelationID(ctx))
	require.Equal(t, "f9b711e3-8f45-4dc3-8cab-07623f5de6e5", logctx.TripID(ctx))
	require.Contains(t, logs.String(), `"causation_id":"command-789"`)
	require.NotContains(t, logs.String(), `"payload"`)
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].Links(), 1)
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", spans[0].Links()[0].SpanContext.TraceID().String())
	require.NotEqual(t, spans[0].SpanContext().TraceID(), spans[0].Links()[0].SpanContext.TraceID())
	require.Contains(t, logs.String(), spans[0].SpanContext().TraceID().String())
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.ConsumeTotal.WithLabelValues("success")), 1e-9)
}

func TestConsumer_OffsetFailureIsNotSuccessfulConsumption(t *testing.T) {
	t.Parallel()
	observer := metrics.New(prometheus.NewRegistry())
	calls := []string{}
	commitError := errors.New("Kafka acknowledgement failed")
	reader := &readerStub{calls: &calls, commitError: commitError, messages: []kafka.Message{{Value: []byte(validTripPublished)}}}
	consumer := &Consumer{reader: reader, handler: &handlerStub{calls: &calls}, handleTimeout: time.Second, metrics: observer, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.ErrorIs(t, consumer.Run(t.Context()), commitError)
	require.InDelta(t, float64(1), testutil.ToFloat64(observer.ConsumeTotal.WithLabelValues("error")), 1e-9)
	require.Zero(t, testutil.ToFloat64(observer.ConsumeTotal.WithLabelValues("success")))
	require.Empty(t, reader.committed)
}

func TestConsumerDoesNotExposeInvalidFieldValue(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	calls := []string{}
	reader := &readerStub{calls: &calls, messages: []kafka.Message{{Value: []byte(strings.Replace(validTripPublished, "2026-10-22T20:00:00Z", "PRIVATE_TEST_VALUE", 1))}}}
	consumer := &Consumer{reader: reader, handler: &handlerStub{calls: &calls}, handleTimeout: time.Second, logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	err := consumer.Run(t.Context())
	require.ErrorContains(t, err, "occurred_at")
	require.NotContains(t, err.Error(), "PRIVATE_TEST_VALUE")
	require.NotContains(t, logs.String(), "PRIVATE_TEST_VALUE")
	require.Contains(t, logs.String(), `"level":"WARN"`)
	require.Empty(t, reader.committed)
	require.Equal(t, []string{"fetch"}, calls)
}

func TestConsumerSkippedEventCommitFailureIsTechnicalError(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	calls := []string{}
	commitError := errors.New("Kafka offset commit failed")
	reader := &readerStub{calls: &calls, commitError: commitError, messages: []kafka.Message{{Value: []byte(`{"event_type":"TripCreated"}`)}}}
	consumer := &Consumer{reader: reader, handler: &handlerStub{calls: &calls}, handleTimeout: time.Second, logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	require.ErrorIs(t, consumer.Run(t.Context()), commitError)
	require.Contains(t, logs.String(), `"level":"ERROR"`)
	require.NotContains(t, logs.String(), `"level":"WARN"`)
	require.Empty(t, reader.committed)
}

func TestConsumerKeepsHeaderMetadataWhenPayloadIsInvalid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed JSON", body: `{"event_id":`},
		{name: "conflicting body metadata", body: strings.Replace(validTripPublished, "78910abc-1234-4567-89ab-1234567890ab", "11111111-1111-4111-8111-111111111111", 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			calls := []string{}
			reader := &readerStub{calls: &calls, messages: []kafka.Message{{Value: []byte(tt.body), Headers: []kafka.Header{
				{Key: "event_id", Value: []byte("78910abc-1234-4567-89ab-1234567890ab")},
				{Key: "event_type", Value: []byte("TripPublished")},
				{Key: "request_id", Value: []byte("request-123")},
				{Key: "correlation_id", Value: []byte("publication-456")},
				{Key: "causation_id", Value: []byte("command-789")},
			}}}}
			consumer := &Consumer{reader: reader, handler: &handlerStub{calls: &calls}, handleTimeout: time.Second, logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			err := consumer.Run(t.Context())
			require.Error(t, err)
			require.Empty(t, reader.committed)
			require.Equal(t, []string{"fetch"}, calls)
			var entry map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &entry))
			require.Equal(t, "WARN", entry["level"])
			require.Equal(t, "78910abc-1234-4567-89ab-1234567890ab", entry["event_id"])
			require.Equal(t, "TripPublished", entry["event_type"])
			require.Equal(t, "request-123", entry["request_id"])
			require.Equal(t, "publication-456", entry["correlation_id"])
			require.Equal(t, "command-789", entry["causation_id"])
		})
	}
}
