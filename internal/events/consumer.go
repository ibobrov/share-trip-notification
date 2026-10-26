package events

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/asaskevich/govalidator/v12"
	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/observability/logctx"
	"github.com/ibobrov/share-trip-notification/internal/observability/metrics"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}
type Consumer struct {
	tracer        trace.Tracer
	reader        messageReader
	handler       service.TripPublishedHandler
	handleTimeout time.Duration
	logger        *slog.Logger
	metrics       *metrics.Metrics
}
type TripPublished struct {
	EventID       string `json:"event_id" valid:"required,uuid"`
	EventType     string `json:"event_type" valid:"required"`
	TripID        string `json:"trip_id" valid:"required,uuid"`
	ClientID      string `json:"client_id" valid:"required,uuid"`
	CompanyID     string `json:"company_id" valid:"required"`
	OccurredAt    string `json:"occurred_at" valid:"required,rfc3339"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	Traceparent   string `json:"traceparent,omitempty"`
	Tracestate    string `json:"tracestate,omitempty"`
}

func NewConsumer(brokers []string, topic, groupID string, handler service.TripPublishedHandler, handleTimeout time.Duration, logger *slog.Logger, observer *metrics.Metrics) *Consumer {
	return &Consumer{
		reader:  kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID, StartOffset: kafka.FirstOffset, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second}),
		handler: handler, handleTimeout: handleTimeout, logger: logger, metrics: observer,
	}
}
func (c *Consumer) Run(ctx context.Context) error {
	if c.tracer == nil {
		c.tracer = otel.Tracer("notification")
	}
	if c.metrics == nil {
		c.metrics = metrics.New(prometheus.NewRegistry())
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		if err := c.process(ctx, message); err != nil {
			return fmt.Errorf("topic=%s partition=%d offset=%d: %w", message.Topic, message.Partition, message.Offset, err)
		}
	}
}
func (c *Consumer) process(ctx context.Context, message kafka.Message) (err error) {
	started := time.Now()
	result := "error"
	validEvent := false
	var event TripPublished
	headers := propagation.MapCarrier{}
	for _, header := range message.Headers {
		headers.Set(header.Key, string(header.Value))
	}
	remote := propagation.TraceContext{}.Extract(ctx, headers)
	options := []trace.SpanStartOption{trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindConsumer)}
	if sc := trace.SpanContextFromContext(remote); sc.IsValid() {
		options = append(options, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	ctx, span := c.tracer.Start(ctx, "ConsumeTripPublished", options...)
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "event handling failed")
		}
		c.metrics.ConsumeTotal.WithLabelValues(result).Inc()
		c.metrics.ConsumeDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
		outcomeCtx := logctx.WithLogger(ctx, c.logger.With("event_id", headers.Get("event_id"), "request_id", headers.Get("request_id"), "correlation_id", headers.Get("correlation_id"), "causation_id", headers.Get("causation_id")))
		level := slog.LevelInfo
		if err != nil {
			level = slog.LevelWarn
			if validEvent {
				level = slog.LevelError
			}
		}
		logctx.Logger(outcomeCtx).Log(outcomeCtx, level, "Kafka handling completed",
			"component", "KafkaConsumer",
			"operation", "ConsumeTripPublished",
			"event_type", headers.Get("event_type"),
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"result", result,
			"error", err,
		)
	}()
	ctx = logctx.WithLogger(ctx, c.logger)
	decodeErr := json.Unmarshal(message.Value, &event)
	// Preserve headers on decode failure; legacy messages fall back to body metadata.
	for key, value := range map[string]string{
		"event_id": event.EventID, "event_type": event.EventType,
		"request_id": event.RequestID, "correlation_id": event.CorrelationID,
		"causation_id": event.CausationID,
	} {
		if headers.Get(key) == "" {
			headers.Set(key, value)
		}
	}
	if headers.Get("correlation_id") == "" {
		headers.Set("correlation_id", headers.Get("event_id"))
	}
	if decodeErr != nil {
		return fmt.Errorf("decode event: %w", decodeErr)
	}
	if event.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	for _, key := range []string{"event_id", "event_type"} {
		expected := event.EventID
		if key == "event_type" {
			expected = event.EventType
		}
		if value := headers.Get(key); value != "" && value != expected {
			return fmt.Errorf("event %s header does not match payload", key)
		}
	}
	// Legacy events without headers remain readable; new headers are authoritative metadata.
	for key, target := range map[string]*string{"request_id": &event.RequestID, "correlation_id": &event.CorrelationID, "causation_id": &event.CausationID} {
		if value := headers.Get(key); value != "" {
			*target = value
		}
	}
	if event.CorrelationID == "" {
		event.CorrelationID = event.EventID
	}
	ctx = logctx.WithRequestID(ctx, event.RequestID)
	ctx = logctx.WithCorrelationID(ctx, event.CorrelationID)
	ctx = logctx.WithBusinessIDs(ctx, event.TripID, event.ClientID)
	ctx = logctx.WithLogger(ctx, c.logger.With("event_id", event.EventID, "request_id", event.RequestID, "correlation_id", event.CorrelationID, "causation_id", event.CausationID))
	span.SetAttributes(attribute.String("event_id", event.EventID), attribute.String("trip_id", event.TripID), attribute.String("correlation_id", event.CorrelationID))
	if event.EventType != "TripPublished" {
		validEvent = true
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit skipped Kafka offset: %w", err)
		}
		result = "ignored"
		return nil
	}
	if _, err := govalidator.ValidateStruct(event); err != nil {
		fields := make([]string, 0)
		for field := range govalidator.ErrorsByField(err) {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		return fmt.Errorf("validate TripPublished: invalid fields [%s]", strings.Join(fields, ", "))
	}
	eventID, err := uuid.Parse(event.EventID)
	if err != nil {
		return fmt.Errorf("parse event_id: %w", err)
	}
	tripID, err := uuid.Parse(event.TripID)
	if err != nil {
		return fmt.Errorf("parse trip_id: %w", err)
	}
	clientID, err := uuid.Parse(event.ClientID)
	if err != nil {
		return fmt.Errorf("parse client_id: %w", err)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("parse occurred_at: %w", err)
	}
	if eventID == uuid.Nil || tripID == uuid.Nil || clientID == uuid.Nil || strings.TrimSpace(event.CompanyID) == "" {
		return fmt.Errorf("event identifiers and company_id must be nonempty")
	}
	validEvent = true
	handlingCtx, cancel := context.WithTimeout(ctx, c.handleTimeout)
	defer cancel()
	response, err := c.handler.HandleTripPublished(handlingCtx, service.HandleTripPublishedRequest{EventID: eventID, TripID: tripID, ClientID: clientID, CompanyID: event.CompanyID, OccurredAt: occurredAt.UTC()})
	if err != nil {
		return fmt.Errorf("handle TripPublished: %w", err)
	}
	if err := c.reader.CommitMessages(handlingCtx, message); err != nil {
		return fmt.Errorf("commit Kafka offset: %w", err)
	}
	result = "success"
	if response.AlreadyProcessed {
		result = "duplicate"
	}
	return nil
}
func (c *Consumer) Close() error { return c.reader.Close() }
