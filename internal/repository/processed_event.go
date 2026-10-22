package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (r *NotificationRepository) TryMarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, processedAt time.Time) (bool, error) {
	result, err := tx.Exec(ctx, `
  INSERT INTO processed_events (event_id, processed_at)
  VALUES ($1, $2)
  ON CONFLICT (event_id) DO NOTHING
 `, eventID, processedAt)
	if err != nil {
		return false, fmt.Errorf("mark event processed: %w", err)
	}
	return result.RowsAffected() == 1, nil
}
