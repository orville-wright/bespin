-- Trading Platform Database Schema
-- PostgreSQL + TimescaleDB for time-series data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Strategies table
CREATE TABLE IF NOT EXISTS strategies (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    strategy_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    symbols TEXT[] NOT NULL,
    parameters JSONB,
    metadata JSONB,
    is_simulation BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id UUID UNIQUE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    
    -- Quantities and prices
    quantity DECIMAL(20, 8) NOT NULL,
    filled_quantity DECIMAL(20, 8) DEFAULT 0,
    price DECIMAL(20, 8),
    stop_price DECIMAL(20, 8),
    avg_fill_price DECIMAL(20, 8),
    
    -- Timing
    time_in_force VARCHAR(10) DEFAULT 'DAY',
    submitted_at TIMESTAMPTZ,
    filled_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    
    -- External references
    broker_order_id VARCHAR(100),
    parent_order_id UUID,
    
    -- Metadata
    metadata JSONB,
    is_simulation BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys and indexes
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    FOREIGN KEY (parent_order_id) REFERENCES orders(order_id)
);

-- Positions table
CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    position_id UUID UNIQUE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'open',
    
    -- Position details
    quantity DECIMAL(20, 8) NOT NULL,
    avg_entry_price DECIMAL(20, 8) NOT NULL,
    current_price DECIMAL(20, 8),
    market_value DECIMAL(20, 2),
    
    -- P&L
    unrealized_pnl DECIMAL(20, 2) DEFAULT 0,
    realized_pnl DECIMAL(20, 2) DEFAULT 0,
    total_pnl DECIMAL(20, 2) DEFAULT 0,
    
    -- Timing
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    
    -- Cost basis and fees
    cost_basis DECIMAL(20, 2) NOT NULL,
    total_fees DECIMAL(20, 2) DEFAULT 0,
    
    -- Risk management
    stop_loss DECIMAL(20, 8),
    take_profit DECIMAL(20, 8),
    
    -- Metadata
    metadata JSONB,
    is_simulation BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id)
);

-- Market data table (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS market_data (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8),
    bid DECIMAL(20, 8),
    ask DECIMAL(20, 8),
    volume BIGINT,
    source VARCHAR(50),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert market_data to hypertable for time-series optimization
SELECT create_hypertable('market_data', 'time', if_not_exists => TRUE);

-- OHLCV bars table (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS ohlcv_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- 1min, 5min, 1hour, 1day, etc.
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume BIGINT NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert ohlcv_bars to hypertable
SELECT create_hypertable('ohlcv_bars', 'time', if_not_exists => TRUE);

-- Strategy performance tracking
CREATE TABLE IF NOT EXISTS strategy_performance (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    total_pnl DECIMAL(20, 2) DEFAULT 0,
    realized_pnl DECIMAL(20, 2) DEFAULT 0,
    unrealized_pnl DECIMAL(20, 2) DEFAULT 0,
    trades_count INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    total_volume DECIMAL(20, 8) DEFAULT 0,
    total_fees DECIMAL(20, 2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    UNIQUE(strategy_id, date)
);

-- Trading signals table
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    signal_id UUID UNIQUE NOT NULL,
    strategy_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('buy', 'sell', 'hold')),
    confidence DECIMAL(5, 4), -- 0.0000 to 1.0000
    price DECIMAL(20, 8),
    quantity DECIMAL(20, 8),
    metadata JSONB,
    generated_at TIMESTAMPTZ NOT NULL,
    executed_at TIMESTAMPTZ,
    order_id UUID,
    
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_submitted_at ON orders(submitted_at);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);

CREATE INDEX IF NOT EXISTS idx_positions_strategy_id ON positions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at);

CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time ON market_data(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_bars_symbol_timeframe_time ON ohlcv_bars(symbol, timeframe, time DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_performance_strategy_date ON strategy_performance(strategy_id, date DESC);

CREATE INDEX IF NOT EXISTS idx_trading_signals_strategy_id ON trading_signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol ON trading_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_trading_signals_generated_at ON trading_signals(generated_at DESC);

-- Views for common queries
CREATE OR REPLACE VIEW active_positions AS
SELECT 
    p.*,
    s.name as strategy_name,
    s.strategy_type,
    (p.current_price - p.avg_entry_price) * p.quantity as unrealized_pnl_calc
FROM positions p
JOIN strategies s ON p.strategy_id = s.strategy_id
WHERE p.status = 'open';

CREATE OR REPLACE VIEW daily_performance AS
SELECT 
    strategy_id,
    DATE(created_at) as date,
    SUM(CASE WHEN status = 'filled' THEN quantity * COALESCE(avg_fill_price, price) ELSE 0 END) as daily_volume,
    COUNT(CASE WHEN status = 'filled' THEN 1 END) as orders_filled,
    COUNT(*) as total_orders
FROM orders
GROUP BY strategy_id, DATE(created_at)
ORDER BY strategy_id, date DESC;

-- Function to update strategy performance daily
CREATE OR REPLACE FUNCTION update_strategy_performance()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO strategy_performance (
        strategy_id, 
        date, 
        total_pnl, 
        realized_pnl, 
        trades_count
    )
    SELECT 
        NEW.strategy_id,
        CURRENT_DATE,
        COALESCE(SUM(total_pnl), 0),
        COALESCE(SUM(realized_pnl), 0),
        COUNT(*)
    FROM positions 
    WHERE strategy_id = NEW.strategy_id 
      AND DATE(created_at) = CURRENT_DATE
    ON CONFLICT (strategy_id, date) 
    DO UPDATE SET
        total_pnl = EXCLUDED.total_pnl,
        realized_pnl = EXCLUDED.realized_pnl,
        trades_count = EXCLUDED.trades_count,
        created_at = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update performance metrics
CREATE TRIGGER trigger_update_strategy_performance
    AFTER INSERT OR UPDATE ON positions
    FOR EACH ROW
    EXECUTE FUNCTION update_strategy_performance();

-- Data retention policies (TimescaleDB)
-- Keep market data for 90 days, OHLCV bars for 2 years
SELECT add_retention_policy('market_data', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('ohlcv_bars', INTERVAL '2 years', if_not_exists => TRUE);

-- Continuous aggregates for OHLCV data (TimescaleDB)
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1hour
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket,
       symbol,
       FIRST(open, time) AS open,
       MAX(high) AS high,
       MIN(low) AS low,
       LAST(close, time) AS close,
       SUM(volume) AS volume
FROM ohlcv_bars
WHERE timeframe = '1min'
GROUP BY bucket, symbol;

-- Add refresh policy for continuous aggregates
SELECT add_continuous_aggregate_policy('ohlcv_1hour',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Comments for documentation
COMMENT ON TABLE strategies IS 'Trading strategies configuration and metadata';
COMMENT ON TABLE orders IS 'All trading orders with lifecycle tracking';
COMMENT ON TABLE positions IS 'Open and closed trading positions with P&L';
COMMENT ON TABLE market_data IS 'Real-time market data feed (TimescaleDB hypertable)';
COMMENT ON TABLE ohlcv_bars IS 'OHLCV candlestick data (TimescaleDB hypertable)';
COMMENT ON TABLE strategy_performance IS 'Daily performance metrics per strategy';
COMMENT ON TABLE trading_signals IS 'Strategy-generated trading signals';

COMMENT ON VIEW active_positions IS 'Currently open positions with strategy details';
COMMENT ON VIEW daily_performance IS 'Daily trading activity summary by strategy';