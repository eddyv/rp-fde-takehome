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
