CREATE TABLE webhook_subscriptions (
    id                    BIGSERIAL PRIMARY KEY,
    url                   VARCHAR(2048) NOT NULL,
    secret                VARCHAR(128) NOT NULL,
    event_type_pattern    VARCHAR(100) NOT NULL DEFAULT '*',
    active                BOOLEAN NOT NULL DEFAULT TRUE,
    consecutive_failures  INTEGER NOT NULL DEFAULT 0,
    created_at            TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_webhook_subscriptions_active ON webhook_subscriptions (active);
