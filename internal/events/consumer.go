package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/asaskevich/govalidator/v12"
	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/segmentio/kafka-go"
)

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader  messageReader
	handler service.TripPublishedHandler
}

type TripPublished struct {
	EventID    string `json:"event_id" valid:"required,uuid"`
	EventType  string `json:"event_type" valid:"required"`
	TripID     string `json:"trip_id" valid:"required,uuid"`
	ClientID   string `json:"client_id" valid:"required,uuid"`
	CompanyID  string `json:"company_id" valid:"required"`
	OccurredAt string `json:"occurred_at" valid:"required,rfc3339"`
}

func NewConsumer(brokers []string, topic, groupID string, handler service.TripPublishedHandler) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: topic, GroupID: groupID,
			StartOffset: kafka.FirstOffset, CommitInterval: 0,
			MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second,
		}),
		handler: handler,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		if err := c.process(ctx, message); err != nil {
			return fmt.Errorf("topic=%s partition=%d offset=%d: %w", message.Topic, message.Partition, message.Offset, err)
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit Kafka offset: %w", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, message kafka.Message) error {
	var envelope struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		return fmt.Errorf("decode event type: %w", err)
	}
	if envelope.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	if envelope.EventType != "TripPublished" {
		return nil
	}
	var event TripPublished
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("decode TripPublished: %w", err)
	}
	if _, err := govalidator.ValidateStruct(event); err != nil {
		return fmt.Errorf("validate TripPublished: %w", err)
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
	handlingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = c.handler.HandleTripPublished(handlingCtx, service.HandleTripPublishedRequest{
		EventID: eventID, TripID: tripID, ClientID: clientID,
		CompanyID: event.CompanyID, OccurredAt: occurredAt.UTC(),
	})
	if err != nil {
		return fmt.Errorf("handle TripPublished: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error { return c.reader.Close() }
