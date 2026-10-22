package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ibobrov/share-trip-notification/internal/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestConsumer_KafkaAndPostgres(t *testing.T) {
	t.Parallel()
	brokerList := os.Getenv("KAFKA_TEST_BROKERS")
	if brokerList == "" {
		t.Skip("set KAFKA_TEST_BROKERS to test real Kafka")
	}
	brokers := strings.Split(brokerList, ",")
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	topic := "notification.consumer.test." + uuid.NewString()
	groupID := "notification-test-" + uuid.NewString()
	connection, err := kafka.DialContext(ctx, "tcp", brokers[0])
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	require.NoError(t, connection.SetDeadline(time.Now().Add(30*time.Second)))
	require.NoError(t, connection.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}))
	defer func() { require.NoError(t, connection.DeleteTopics(topic)) }()
	_, err = connection.ReadPartitions(topic)
	require.NoError(t, err)
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: time.Second}
	// This group belongs only to this test and is deleted after the reader leaves it.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		result, err := client.DeleteGroups(cleanupCtx, &kafka.DeleteGroupsRequest{GroupIDs: []string{groupID}})
		require.NoError(t, err)
		if result != nil {
			require.NoError(t, result.Errors[groupID])
		}
	}()
	consumer := events.NewConsumer(brokers, topic, groupID, testNotificationService)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(consumerCtx) }()
	defer func() {
		stopConsumer()
		err := <-done
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "unexpected consumer error: %v", err)
		require.NoError(t, consumer.Close())
	}()
	request := newTripPublishedRequest()
	data, err := json.Marshal(events.TripPublished{
		EventID: request.EventID.String(), EventType: "TripPublished", TripID: request.TripID.String(),
		ClientID: request.ClientID.String(), CompanyID: request.CompanyID,
		OccurredAt: request.OccurredAt.Format(time.RFC3339Nano),
	})
	require.NoError(t, err)
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 3}
	defer func() { require.NoError(t, writer.Close()) }()
	message := kafka.Message{Key: []byte(request.TripID.String()), Value: data}
	require.NoError(t, writer.WriteMessages(ctx, message, message, kafka.Message{Value: []byte(`{"event_type":"TripCreated"}`)}))
	require.Eventually(t, func() bool {
		result, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: groupID, Topics: map[string][]int{topic: {0}}})
		if err != nil || result.Error != nil {
			return false
		}
		partitions := result.Topics[topic]
		return len(partitions) == 1 && partitions[0].Error == nil && partitions[0].CommittedOffset == 3
	}, 15*time.Second, 100*time.Millisecond, "consumer must commit both duplicate deliveries and ignored event")
	requireEventCounts(t, request, 1, 1)
}
