CREATE TABLE notification_deliveries (
    id          BIGSERIAL PRIMARY KEY,
    event_id    VARCHAR(64) NOT NULL,
    channel     VARCHAR(30) NOT NULL,
    target      VARCHAR(255) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_notification_deliveries_event_channel_target
    ON notification_deliveries (event_id, channel, target);
