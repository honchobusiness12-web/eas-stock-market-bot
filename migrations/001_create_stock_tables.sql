-- EAS Stock Market Bot — Phase 1 Schema
-- All tables use the market_ prefix.
-- NO foreign key constraints reference the ranked players table.
-- This file is kept for reference; the bot creates tables directly in init_db().

-- User balances, daily claims, and wealth roles.
-- guild_id + user_id are standalone — no FK to players.
CREATE TABLE IF NOT EXISTS market_users (
  guild_id    BIGINT NOT NULL,
  user_id     BIGINT NOT NULL,
  balance     BIGINT NOT NULL DEFAULT 250000,
  last_daily  TIMESTAMP,
  wealth_role TEXT,
  PRIMARY KEY (guild_id, user_id)
);

-- Top 10 player stock listings per guild.
CREATE TABLE IF NOT EXISTS market_stocks (
  guild_id               BIGINT NOT NULL,
  player_id              BIGINT NOT NULL,
  price                  BIGINT NOT NULL DEFAULT 1000,
  rank_position          INTEGER NOT NULL,
  active                 BOOLEAN NOT NULL DEFAULT TRUE,
  cr                     INTEGER NOT NULL DEFAULT 0,
  wins                   INTEGER NOT NULL DEFAULT 0,
  losses                 INTEGER NOT NULL DEFAULT 0,
  kills                  INTEGER NOT NULL DEFAULT 0,
  mvps                   INTEGER NOT NULL DEFAULT 0,
  streak                 INTEGER NOT NULL DEFAULT 0,
  previous_rank_position INTEGER,
  updated_at             TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (guild_id, player_id)
);

-- User share holdings.
CREATE TABLE IF NOT EXISTS market_holdings (
  guild_id      BIGINT NOT NULL,
  investor_id   BIGINT NOT NULL,
  player_id     BIGINT NOT NULL,
  shares        INTEGER NOT NULL DEFAULT 0,
  average_price BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, investor_id, player_id)
);

-- Buy/sell/daily transaction history.
CREATE TABLE IF NOT EXISTS market_transactions (
  id          SERIAL PRIMARY KEY,
  guild_id    BIGINT NOT NULL,
  investor_id BIGINT NOT NULL,
  player_id   BIGINT,
  type        TEXT NOT NULL,
  shares      INTEGER,
  price       BIGINT,
  total       BIGINT,
  created_at  TIMESTAMP DEFAULT NOW()
);

-- Stock price change audit log.
CREATE TABLE IF NOT EXISTS market_stock_history (
  id         SERIAL PRIMARY KEY,
  guild_id   BIGINT NOT NULL,
  player_id  BIGINT NOT NULL,
  old_price  BIGINT NOT NULL,
  new_price  BIGINT NOT NULL,
  reason     TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Per-guild market open/close flag.
CREATE TABLE IF NOT EXISTS market_settings (
  guild_id    BIGINT PRIMARY KEY,
  market_open BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at  TIMESTAMP DEFAULT NOW()
);

-- Global shop item catalogue.
CREATE TABLE IF NOT EXISTS market_shop_items (
  id          SERIAL PRIMARY KEY,
  item_name   TEXT NOT NULL UNIQUE,
  description TEXT,
  price       BIGINT NOT NULL,
  category    TEXT NOT NULL,
  created_at  TIMESTAMP DEFAULT NOW()
);

-- Items purchased by users per guild.
CREATE TABLE IF NOT EXISTS market_user_items (
  id           SERIAL PRIMARY KEY,
  guild_id     BIGINT NOT NULL,
  user_id      BIGINT NOT NULL,
  item_id      INTEGER NOT NULL,
  purchased_at TIMESTAMP DEFAULT NOW()
);

-- Wealth role assignment history.
CREATE TABLE IF NOT EXISTS market_wealth_roles (
  id          SERIAL PRIMARY KEY,
  guild_id    BIGINT NOT NULL,
  user_id     BIGINT NOT NULL,
  role_name   TEXT NOT NULL,
  threshold   BIGINT NOT NULL,
  assigned_at TIMESTAMP DEFAULT NOW()
);

-- Performance indexes.
CREATE INDEX IF NOT EXISTS idx_market_users_guild      ON market_users(guild_id);
CREATE INDEX IF NOT EXISTS idx_market_stocks_guild     ON market_stocks(guild_id, active);
CREATE INDEX IF NOT EXISTS idx_market_holdings_inv     ON market_holdings(guild_id, investor_id);
CREATE INDEX IF NOT EXISTS idx_market_transactions_inv ON market_transactions(guild_id, investor_id);
CREATE INDEX IF NOT EXISTS idx_market_stock_history    ON market_stock_history(guild_id, player_id);
CREATE INDEX IF NOT EXISTS idx_market_user_items       ON market_user_items(guild_id, user_id);
CREATE INDEX IF NOT EXISTS idx_market_wealth_roles     ON market_wealth_roles(guild_id, user_id);
