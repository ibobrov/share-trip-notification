package events

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

const validTripPublished = `{"event_id":"78910abc-1234-4567-89ab-1234567890ab","event_type":"TripPublished","trip_id":"f9b711e3-8f45-4dc3-8cab-07623f5de6e5","client_id":"a1b2c3d4-1111-4111-8111-123456789abc","company_id":"company-acme","occurred_at":"2026-10-22T20:00:00Z"}`

type readerStub struct {
	messages    []kafka.Message
	calls       *[]string
	commitError error
	committed   []kafka.Message
	closed      bool
}

func (r *readerStub) FetchMessage(context.Context) (kafka.Message, error) {
	*r.calls = append(*r.calls, "fetch")
	if len(r.messages) == 0 {
		return kafka.Message{}, io.EOF
	}
	message := r.messages[0]
	r.messages = r.messages[1:]
	return message, nil
}
func (r *readerStub) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	*r.calls = append(*r.calls, "commit")
	if r.commitError == nil {
		r.committed = append(r.committed, messages...)
	}
	return r.commitError
}
func (r *readerStub) Close() error { r.closed = true; return nil }

type handlerStub struct {
	calls    *[]string
	requests []service.HandleTripPublishedRequest
	err      error
}

func (h *handlerStub) HandleTripPublished(_ context.Context, request service.HandleTripPublishedRequest) (service.HandleTripPublishedResponse, error) {
	*h.calls = append(*h.calls, "handle")
	h.requests = append(h.requests, request)
	return service.HandleTripPublishedResponse{}, h.err
}

func TestConsumer_Run(t *testing.T) {
	t.Parallel()
	databaseError := errors.New("database unavailable")
	commitError := errors.New("offset commit failed")
	tests := []struct {
		name         string
		body         string
		handlerError error
		commitError  error
		wantError    error
		wantCalls    []string
		wantCommits  int
	}{
		{name: "handle before commit", body: validTripPublished, wantError: io.EOF, wantCalls: []string{"fetch", "handle", "commit", "fetch"}, wantCommits: 1},
		{name: "ignore another event type", body: `{"event_type":"TripCreated"}`, wantError: io.EOF, wantCalls: []string{"fetch", "commit", "fetch"}, wantCommits: 1},
		{name: "handler failure must not commit", body: validTripPublished, handlerError: databaseError, wantError: databaseError, wantCalls: []string{"fetch", "handle"}},
		{name: "offset failure stops consumption", body: validTripPublished, commitError: commitError, wantError: commitError, wantCalls: []string{"fetch", "handle", "commit"}},
		{name: "invalid JSON must not commit", body: `{`, wantCalls: []string{"fetch"}},
		{name: "missing event type must not commit", body: `{}`, wantCalls: []string{"fetch"}},
		{name: "missing payload must not commit", body: `{"event_type":"TripPublished"}`, wantCalls: []string{"fetch"}},
		{name: "zero UUID must not commit", body: strings.Replace(validTripPublished, "78910abc-1234-4567-89ab-1234567890ab", "00000000-0000-0000-0000-000000000000", 1), wantCalls: []string{"fetch"}},
		{name: "invalid UUID must not commit", body: strings.Replace(validTripPublished, "78910abc-1234-4567-89ab-1234567890ab", "bad-id", 1), wantCalls: []string{"fetch"}},
		{name: "blank company must not commit", body: strings.Replace(validTripPublished, "company-acme", "   ", 1), wantCalls: []string{"fetch"}},
		{name: "invalid date must not commit", body: strings.Replace(validTripPublished, "2026-10-22T20:00:00Z", "not-a-date", 1), wantCalls: []string{"fetch"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := []string{}
			message := kafka.Message{Topic: "trip.events", Partition: 0, Offset: 42, Value: []byte(tt.body)}
			reader := &readerStub{messages: []kafka.Message{message}, calls: &calls, commitError: tt.commitError}
			handler := &handlerStub{calls: &calls, err: tt.handlerError}
			consumer := &Consumer{reader: reader, handler: handler}
			err := consumer.Run(t.Context())
			require.Error(t, err)
			if tt.wantError != nil {
				require.ErrorIs(t, err, tt.wantError)
			}
			require.Equal(t, tt.wantCalls, calls)
			require.Len(t, reader.committed, tt.wantCommits)
			if len(handler.requests) > 0 {
				request := handler.requests[0]
				require.Equal(t, "78910abc-1234-4567-89ab-1234567890ab", request.EventID.String())
				require.Equal(t, "f9b711e3-8f45-4dc3-8cab-07623f5de6e5", request.TripID.String())
				require.Equal(t, "a1b2c3d4-1111-4111-8111-123456789abc", request.ClientID.String())
				require.Equal(t, "company-acme", request.CompanyID)
				require.Equal(t, "2026-10-22T20:00:00Z", request.OccurredAt.Format("2006-01-02T15:04:05Z07:00"))
			}
			require.NoError(t, consumer.Close())
			require.True(t, reader.closed)
		})
	}
}
