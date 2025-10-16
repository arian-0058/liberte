-- Phase 1 schema: signals & paper_trades
CREATE TABLE IF NOT EXISTS signals (
  id SERIAL PRIMARY KEY,
  token TEXT NOT NULL,
  liquidity DOUBLE PRECISION,
  creator_share DOUBLE PRECISION,
  volume_5m DOUBLE PRECISION,
  timestamp TIMESTAMP DEFAULT NOW(),
  score INT,
  passed BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp);

CREATE TABLE IF NOT EXISTS paper_trades (
  id SERIAL PRIMARY KEY,
  token TEXT NOT NULL,
  buy_price DOUBLE PRECISION,
  current_price DOUBLE PRECISION,
  roi DOUBLE PRECISION,
  time_elapsed INTERVAL,
  status TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_trades_token ON paper_trades(token);
