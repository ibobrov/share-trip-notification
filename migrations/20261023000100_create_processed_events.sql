-- +goose Up
CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
DROP TABLE processed_events;
