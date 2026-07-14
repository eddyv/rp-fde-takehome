CREATE TABLE IF NOT EXISTS edits (
    id           TEXT PRIMARY KEY,      -- Wikipedia recent-change id
    title        TEXT,                  -- nullable: failed rows may lack fields
    editor       TEXT,
    comment      TEXT,
    byte_delta   INT,
    label        TEXT,                  -- vandalism | substantive | trivia | unclear
    confidence   REAL,
    reasoning    TEXT,
    model        TEXT,
    status       TEXT NOT NULL DEFAULT 'classified'
                 CHECK (status IN ('classified', 'failed')),
    event_time   TIMESTAMPTZ,           -- when the edit happened on Wikipedia
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS edits_label_idx ON edits (label);
CREATE INDEX IF NOT EXISTS edits_status_idx ON edits (status);

-- Keyset pagination scans on (processed_at DESC, id DESC). A time-ordered
-- UUIDv7 PK would collapse this to a single column, but native uuidv7()
-- needs Postgres 18 (or an extension) -- we're staying on 16 for now.
CREATE INDEX IF NOT EXISTS edits_processed_at_id_idx ON edits (processed_at DESC, id DESC);
