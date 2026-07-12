CREATE TABLE IF NOT EXISTS edits (
    id           TEXT PRIMARY KEY,      -- Wikipedia recent-change id
    title        TEXT NOT NULL,
    editor       TEXT,
    comment      TEXT,
    byte_delta   INT,
    label        TEXT,                  -- vandalism | substantive | trivia | unclear
    confidence   REAL,
    reasoning    TEXT,
    model        TEXT,
    event_time   TIMESTAMPTZ,           -- when the edit happened on Wikipedia
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS edits_label_idx ON edits (label);
