-- SQL script for phase 2 initialisation
-- This script defines the ROI history and score tables as per the HLD.

CREATE TABLE IF NOT EXISTS roi_history (
  id SERIAL PRIMARY KEY,
  token TEXT UNIQUE,
  roi_5m DOUBLE PRECISION,
  roi_15m DOUBLE PRECISION,
  roi_1h DOUBLE PRECISION,
  volatility DOUBLE PRECISION,
  roi_slope DOUBLE PRECISION,
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scores (
  id SERIAL PRIMARY KEY,
  token TEXT UNIQUE,
  avg_roi DOUBLE PRECISION,
  volatility DOUBLE PRECISION,
  trend DOUBLE PRECISION,
  score DOUBLE PRECISION,
  confidence DOUBLE PRECISION,
  updated_at TIMESTAMP DEFAULT NOW()
);