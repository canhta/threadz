-- Schema v13 -> v14. One-off; run against a STOPPED service, after backing up
-- the database:  sqlite3 -bail threadz.db < migrate-14.sql
-- -bail is required: it stops the script at the first error. The file refuses
-- to run on any other schema version and is one transaction: it applies
-- completely or not at all. It adds the account insights tables and the
-- account_reconciliation view and adds account_followers.source; no row changes.
-- The first run of the new binary backfills account_daily from the API.
--
-- The store test (TestMigrate14MatchesFreshSchema) builds v13 from
-- internal/store/testdata/schema-v13.sql, applies this file and compares the
-- result with a fresh v14 database; keep the two in step.

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
    settled INTEGER NOT NULL DEFAULT 0 CHECK (settled IN (0, 1)),
    fetched_at TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, day, metric)
);

CREATE VIEW account_reconciliation AS
SELECT agent_id, day, account_views, attributed, account_views - attributed AS residual
FROM (
    SELECT d.agent_id AS agent_id, d.day AS day, d.value AS account_views,
           COALESCE((SELECT SUM(MAX(0,
                  COALESCE((SELECT m.views FROM metrics m WHERE m.action_id = x.id AND julianday(m.measured_at) <= julianday(d.ends_at) ORDER BY julianday(m.measured_at) DESC LIMIT 1), 0)
                - COALESCE((SELECT m.views FROM metrics m WHERE m.action_id = x.id AND julianday(m.measured_at) < julianday(d.starts_at) ORDER BY julianday(m.measured_at) DESC LIMIT 1), 0)))
                FROM actions x WHERE x.agent_id = d.agent_id AND x.dry_run = 0), 0) AS attributed
    FROM account_daily d
    WHERE d.metric = 'views'
);

-- Append further v14 changes above this line.

PRAGMA user_version = 14;

COMMIT;
