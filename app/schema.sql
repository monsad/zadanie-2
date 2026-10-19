-- One table is the inbox, the queue and the result store, so accepting
-- and enqueueing an event is a single atomic INSERT.
CREATE TABLE IF NOT EXISTS webhook_events (
    id               INTEGER PRIMARY KEY,
    idempotency_key  TEXT    NOT NULL UNIQUE,
    source           TEXT    NOT NULL,
    event_type       TEXT    NOT NULL,
    payload          TEXT    NOT NULL,          -- canonical JSON
    payload_hash     TEXT    NOT NULL,          -- sha256 of type + payload
    status           TEXT    NOT NULL
                     CHECK (status IN ('pending', 'processing',
                                       'retry_scheduled', 'completed',
                                       'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    next_attempt_at  REAL    NOT NULL,          -- unix epoch seconds
    lease_token      TEXT,
    locked_until     REAL,
    locked_by        TEXT,
    last_error       TEXT,
    result           TEXT,                      -- JSON
    received_at      REAL    NOT NULL,
    started_at       REAL,
    completed_at     REAL,
    updated_at       REAL    NOT NULL
);

-- Claim and dispatch lag: ordered by readiness, only runnable rows.
CREATE INDEX IF NOT EXISTS ix_events_ready
    ON webhook_events (next_attempt_at)
    WHERE status IN ('pending', 'retry_scheduled');

-- Lease recovery: only rows currently leased.
CREATE INDEX IF NOT EXISTS ix_events_lease
    ON webhook_events (locked_until)
    WHERE status = 'processing';

-- Queue depth per status and retention by completion time.
CREATE INDEX IF NOT EXISTS ix_events_status_completed
    ON webhook_events (status, completed_at);

-- Keys outlive payload rows so a retry after DELETE/purge is not a new job.
CREATE TABLE IF NOT EXISTS webhook_idempotency_keys (
    idempotency_key  TEXT PRIMARY KEY,
    payload_hash     TEXT NOT NULL,
    event_id         INTEGER REFERENCES webhook_events(id)
                     ON DELETE SET NULL,
    last_status      TEXT NOT NULL
                     CHECK (last_status IN ('pending', 'processing',
                                            'retry_scheduled', 'completed',
                                            'dead')),
    source           TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    created_at       REAL NOT NULL,
    expires_at       REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_idempotency_keys_expires
    ON webhook_idempotency_keys (expires_at);

PRAGMA user_version = 2;
