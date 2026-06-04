-- Stock Market Bot Tables
-- These tables are created in the SAME database as the ranked system
-- They do NOT modify or touch any existing ranked data

-- StarPoints balances for each player
CREATE TABLE IF NOT EXISTS star_points (
  id SERIAL PRIMARY KEY,
  user_id TEXT NOT NULL UNIQUE,
  balance DECIMAL(15, 2) NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES players(user_id) ON DELETE CASCADE
);

-- Stock holdings for each player
CREATE TABLE IF NOT EXISTS holdings (
  id SERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  stock_symbol TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 0,
  average_cost DECIMAL(15, 2) NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, stock_symbol),
  FOREIGN KEY (user_id) REFERENCES players(user_id) ON DELETE CASCADE
);

-- Stock prices (one per player stock)
CREATE TABLE IF NOT EXISTS stock_prices (
  id SERIAL PRIMARY KEY,
  stock_symbol TEXT NOT NULL UNIQUE,
  player_user_id TEXT NOT NULL,
  current_price DECIMAL(15, 2) NOT NULL DEFAULT 100,
  previous_price DECIMAL(15, 2) NOT NULL DEFAULT 100,
  price_change DECIMAL(15, 2) NOT NULL DEFAULT 0,
  percent_change DECIMAL(5, 2) NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (player_user_id) REFERENCES players(user_id) ON DELETE CASCADE
);

-- Transaction history
CREATE TABLE IF NOT EXISTS transactions (
  id SERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  transaction_type TEXT NOT NULL, -- 'buy', 'sell', 'dividend', 'transfer'
  stock_symbol TEXT,
  quantity INTEGER,
  price_per_unit DECIMAL(15, 2),
  total_amount DECIMAL(15, 2) NOT NULL,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES players(user_id) ON DELETE CASCADE
);

-- Stock price history for charting
CREATE TABLE IF NOT EXISTS stock_history (
  id SERIAL PRIMARY KEY,
  stock_symbol TEXT NOT NULL,
  price DECIMAL(15, 2) NOT NULL,
  volume INTEGER DEFAULT 0,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_star_points_user_id ON star_points(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_stock ON holdings(stock_symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices(stock_symbol);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_stock_history_symbol ON stock_history(stock_symbol);
CREATE INDEX IF NOT EXISTS idx_stock_history_timestamp ON stock_history(timestamp);
