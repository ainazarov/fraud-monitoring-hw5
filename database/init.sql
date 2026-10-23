CREATE TABLE IF NOT EXISTS scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL CHECK (length(model_version) > 0),
    us_state TEXT,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION CHECK (amount >= 0),
    transaction_time TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    emitted_at TIMESTAMPTZ,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    sequence BIGSERIAL UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS scores_version_idx ON scores (model_version, sequence DESC);
-- Idempotent migration also supports a persistent volume from an earlier run.
ALTER TABLE scores ADD COLUMN IF NOT EXISTS us_state TEXT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS merch TEXT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS cat_id TEXT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION CHECK (amount >= 0);
ALTER TABLE scores ADD COLUMN IF NOT EXISTS transaction_time TIMESTAMPTZ;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS emitted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS scores_processed_at_idx ON scores (processed_at DESC);
CREATE INDEX IF NOT EXISTS scores_scored_at_idx ON scores (scored_at DESC);
CREATE INDEX IF NOT EXISTS scores_business_idx ON scores (us_state, merch, cat_id, scored_at DESC);
