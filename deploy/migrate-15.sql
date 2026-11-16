-- Schema v13 -> v15. One-off; run against a STOPPED service, after backing up
-- the database:  sqlite3 -bail threadz.db < migrate-15.sql
-- -bail is required: it stops the script at the first error. The file refuses
-- to run on any other schema version and is one transaction: it applies
-- completely or not at all. It adds the account insights tables, adds
-- account_followers.source, and rebuilds
-- metric_jobs with window_minutes (the old window_hours times 60), the new
-- offset list and the settled reason; every job and sample is kept. The first run of
-- the new binary backfills account_daily from the API and seeds the new offsets.
--
-- The store test (TestMigrate15MatchesFreshSchema) builds v13 from
-- internal/store/testdata/schema-v13.sql, applies this file and compares the
-- result with a fresh v15 database; keep the two in step.

BEGIN IMMEDIATE;

-- Abort unless this is a v13 database: a failing CHECK-style insert into a
-- temp table is the portable way to stop a sqlite3 script.
CREATE TEMP TABLE migrate_guard(ok INTEGER NOT NULL CHECK (ok = 1));
INSERT INTO migrate_guard SELECT (SELECT user_version FROM pragma_user_version) = 13;
DROP TABLE migrate_guard;

ALTER TABLE account_followers ADD COLUMN source TEXT NOT NULL DEFAULT '';

CREATE TABLE account_samples (
    agent_id TEXT NOT NULL,
    measured_at TEXT NOT NULL,
    metric TEXT NOT NULL,
    since TEXT NOT NULL,
    until TEXT NOT NULL,
    value INTEGER NOT NULL CHECK (value >= 0),
    source TEXT NOT NULL,
    PRIMARY KEY (agent_id, measured_at, metric, since, until)
);

CREATE TABLE account_daily (
    agent_id TEXT NOT NULL,
    day TEXT NOT NULL,
    metric TEXT NOT NULL,
    value INTEGER CHECK (value IS NULL OR value >= 0),
    fetched_at TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, day, metric)
);

ALTER TABLE metric_jobs RENAME TO metric_jobs_v13;

CREATE TABLE metric_jobs (
    agent_id TEXT NOT NULL,
    action_id INTEGER NOT NULL,
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN (15,60,180,360,720,1440,2880,5760,10080)),
    due_at TEXT NOT NULL,
    not_before TEXT NOT NULL,
    claim_version INTEGER NOT NULL DEFAULT 0 CHECK(claim_version >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    claimed_at TEXT,
    lease_until TEXT,
    completed_at TEXT,
    reason TEXT NOT NULL DEFAULT '' CHECK(reason IN ('','metrics_unavailable','source_unavailable','read_failed','binding_mismatch','auth_required','cancelled','persistence_failed','measurement_failed','media_gone','settled')),
    PRIMARY KEY(agent_id,action_id,window_minutes),
    CHECK((claimed_at IS NULL) = (lease_until IS NULL)),
    CHECK(lease_until IS NULL OR lease_until > claimed_at),
    CHECK(completed_at IS NULL OR lease_until IS NULL)
);

INSERT INTO metric_jobs(agent_id,action_id,window_minutes,due_at,not_before,claim_version,attempts,claimed_at,lease_until,completed_at,reason)
SELECT agent_id,action_id,window_hours*60,due_at,not_before,claim_version,attempts,claimed_at,lease_until,completed_at,reason FROM metric_jobs_v13;

DROP TABLE metric_jobs_v13;

CREATE INDEX metric_jobs_pending ON metric_jobs(agent_id,completed_at,not_before,due_at);

-- Append further v15 changes above this line.

PRAGMA user_version = 15;

COMMIT;
