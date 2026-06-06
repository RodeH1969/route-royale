-- Run this once to set up the database

CREATE TABLE IF NOT EXISTS sessions (
  id SERIAL PRIMARY KEY,
  route_id VARCHAR(20) NOT NULL DEFAULT '379-4799',
  date DATE NOT NULL DEFAULT CURRENT_DATE,
  status VARCHAR(20) NOT NULL DEFAULT 'waiting',
  win_probability INTEGER NOT NULL DEFAULT 50,
  trip_id VARCHAR(100),
  trip_departure VARCHAR(20),
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS elim_rules (
  id SERIAL PRIMARY KEY,
  session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
  stop_index INTEGER NOT NULL,
  -- which stop index (0-based) triggers this rule
  call_text VARCHAR(100) NOT NULL,
  -- e.g. "♣ Clubs Out!"
  kills_suit CHAR(1),
  -- ♠ ♥ ♦ ♣ or null
  kills_ranks VARCHAR(50),
  -- comma-separated e.g. "2,3,4" or null
  is_final BOOLEAN DEFAULT FALSE,
  sort_order INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
  id SERIAL PRIMARY KEY,
  session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
  name VARCHAR(12) NOT NULL,
  phone VARCHAR(20) NOT NULL,
  card_rank VARCHAR(3) NOT NULL,
  -- 2-10, J, Q, K, A
  card_suit CHAR(1) NOT NULL,
  -- ♠ ♥ ♦ ♣
  status VARCHAR(20) NOT NULL DEFAULT 'alive',
  -- alive | eliminated | winner
  eliminated_at_stop INTEGER,
  joined_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(session_id, phone)
);

CREATE TABLE IF NOT EXISTS stop_events (
  id SERIAL PRIMARY KEY,
  session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
  stop_index INTEGER NOT NULL,
  stop_name VARCHAR(100),
  triggered_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast player lookups
CREATE INDEX IF NOT EXISTS idx_players_session ON players(session_id);
CREATE INDEX IF NOT EXISTS idx_players_phone ON players(session_id, phone);