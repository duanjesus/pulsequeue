ALTER TABLE processed_events ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0;

-- When the producer says the event happened. Needed to rebuild the exact same event on replay;
-- rows recorded before this column existed stay NULL and fall back to received_at.
ALTER TABLE processed_events ADD COLUMN occurred_at TIMESTAMP;
