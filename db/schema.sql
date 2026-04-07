-- ============================================================
-- Openclaw Dashboard — DB Schema v2 (multi-node)
-- Node.js built-in sqlite (node:sqlite), no external deps
-- ============================================================

-- Token consumption events, one row per agent LLM message
CREATE TABLE IF NOT EXISTS token_events (
  id                 TEXT    PRIMARY KEY,
  node_id            TEXT    NOT NULL DEFAULT 'unknown',
  agent_id           TEXT    NOT NULL,
  session_id         TEXT    NOT NULL,
  ts                 INTEGER NOT NULL,
  model              TEXT,
  input_tokens       INTEGER DEFAULT 0,
  output_tokens      INTEGER DEFAULT 0,
  cache_read_tokens  INTEGER DEFAULT 0,
  cache_write_tokens INTEGER DEFAULT 0,
  cost               REAL    DEFAULT 0
);

-- Filtered queries (most common hot path: node + time range)
CREATE INDEX IF NOT EXISTS idx_node_agent_ts  ON token_events (node_id, agent_id, ts);
CREATE INDEX IF NOT EXISTS idx_agent_ts       ON token_events (agent_id, ts);
CREATE INDEX IF NOT EXISTS idx_session        ON token_events (session_id);

-- Projects table (future Trello-style board)
CREATE TABLE IF NOT EXISTS projects (
  id          TEXT    PRIMARY KEY,
  title       TEXT    NOT NULL,
  description TEXT,
  status      TEXT    NOT NULL DEFAULT 'backlog', -- 'backlog'|'wip'|'review'|'done'
  agent_id    TEXT,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS project_cards (
  id          TEXT    PRIMARY KEY,
  project_id  TEXT    NOT NULL REFERENCES projects(id),
  title       TEXT    NOT NULL,
  description TEXT,
  status      TEXT    NOT NULL DEFAULT 'backlog',
  position    INTEGER DEFAULT 0,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);
