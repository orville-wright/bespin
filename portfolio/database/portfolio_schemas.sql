-- Portfolio Management Database Schemas
-- Requires PostgreSQL with TimescaleDB extension for time-series data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Instruments/Assets table
CREATE TABLE IF NOT EXISTS instruments (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    instrument_type VARCHAR(20) NOT NULL, -- 'stock', 'option', 'future', 'crypto', 'bond', 'fx'
    exchange VARCHAR(10),
    sector VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    multiplier DECIMAL(10,2) DEFAULT 1.0,
    tick_size DECIMAL(10,6) DEFAULT 0.01,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Portfolios table
CREATE TABLE IF NOT EXISTS portfolios (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    base_currency VARCHAR(3) DEFAULT 'USD',
    initial_capital DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Positions table (time-series for historical tracking)
CREATE TABLE IF NOT EXISTS positions (
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    quantity DECIMAL(15,4) NOT NULL,
    average_cost DECIMAL(15,6) NOT NULL,
    market_value DECIMAL(15,2),
    unrealized_pnl DECIMAL(15,2),
    realized_pnl DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (timestamp, portfolio_id, instrument_id),
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Convert positions to hypertable for time-series optimization
SELECT create_hypertable('positions', 'timestamp', if_not_exists => TRUE);

-- Transactions/Trades table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    portfolio_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    transaction_type VARCHAR(10) NOT NULL, -- 'BUY', 'SELL', 'DIVIDEND', 'SPLIT'
    quantity DECIMAL(15,4) NOT NULL,
    price DECIMAL(15,6) NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL, -- quantity * price + fees
    fees DECIMAL(10,2) DEFAULT 0,
    commission DECIMAL(10,2) DEFAULT 0,
    execution_time TIMESTAMPTZ NOT NULL,
    order_id VARCHAR(50),
    strategy_name VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Market data table (time-series)
CREATE TABLE IF NOT EXISTS market_data (
    timestamp TIMESTAMPTZ NOT NULL,
    instrument_id INTEGER NOT NULL,
    open_price DECIMAL(15,6),
    high_price DECIMAL(15,6),
    low_price DECIMAL(15,6),
    close_price DECIMAL(15,6) NOT NULL,
    volume BIGINT,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    data_source VARCHAR(20),
    PRIMARY KEY (timestamp, instrument_id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Convert market_data to hypertable
SELECT create_hypertable('market_data', 'timestamp', if_not_exists => TRUE);

-- Portfolio performance metrics (time-series)
CREATE TABLE IF NOT EXISTS portfolio_performance (
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_id INTEGER NOT NULL,
    total_value DECIMAL(15,2) NOT NULL,
    cash_balance DECIMAL(15,2) NOT NULL,
    invested_amount DECIMAL(15,2) NOT NULL,
    total_return DECIMAL(15,2) NOT NULL,
    daily_return DECIMAL(10,6),
    cumulative_return DECIMAL(10,6),
    sharpe_ratio DECIMAL(10,6),
    max_drawdown DECIMAL(10,6),
    volatility DECIMAL(10,6),
    beta DECIMAL(10,6),
    alpha DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (timestamp, portfolio_id),
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
);

-- Convert portfolio_performance to hypertable
SELECT create_hypertable('portfolio_performance', 'timestamp', if_not_exists => TRUE);

-- Risk metrics table
CREATE TABLE IF NOT EXISTS risk_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_id INTEGER NOT NULL,
    var_95 DECIMAL(15,2), -- Value at Risk 95%
    var_99 DECIMAL(15,2), -- Value at Risk 99%
    cvar_95 DECIMAL(15,2), -- Conditional VaR 95%
    cvar_99 DECIMAL(15,2), -- Conditional VaR 99%
    tracking_error DECIMAL(10,6),
    information_ratio DECIMAL(10,6),
    sortino_ratio DECIMAL(10,6),
    calmar_ratio DECIMAL(10,6),
    maximum_drawdown DECIMAL(10,6),
    drawdown_duration INTEGER, -- days
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (timestamp, portfolio_id),
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
);

-- Convert risk_metrics to hypertable
SELECT create_hypertable('risk_metrics', 'timestamp', if_not_exists => TRUE);

-- Benchmark data for performance comparison
CREATE TABLE IF NOT EXISTS benchmarks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Benchmark performance data
CREATE TABLE IF NOT EXISTS benchmark_performance (
    timestamp TIMESTAMPTZ NOT NULL,
    benchmark_id INTEGER NOT NULL,
    price DECIMAL(15,6) NOT NULL,
    daily_return DECIMAL(10,6),
    cumulative_return DECIMAL(10,6),
    PRIMARY KEY (timestamp, benchmark_id),
    FOREIGN KEY (benchmark_id) REFERENCES benchmarks(id)
);

-- Convert benchmark_performance to hypertable
SELECT create_hypertable('benchmark_performance', 'timestamp', if_not_exists => TRUE);

-- Attribution analysis table
CREATE TABLE IF NOT EXISTS attribution_analysis (
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_id INTEGER NOT NULL,
    instrument_id INTEGER,
    sector VARCHAR(50),
    attribution_type VARCHAR(20), -- 'stock_selection', 'sector_allocation', 'currency', 'timing'
    contribution DECIMAL(10,6) NOT NULL,
    weight DECIMAL(8,6),
    benchmark_weight DECIMAL(8,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (timestamp, portfolio_id, COALESCE(instrument_id, 0), attribution_type),
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Convert attribution_analysis to hypertable
SELECT create_hypertable('attribution_analysis', 'timestamp', if_not_exists => TRUE);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_positions_portfolio_timestamp ON positions (portfolio_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_positions_instrument ON positions (instrument_id);
CREATE INDEX IF NOT EXISTS idx_transactions_portfolio_time ON transactions (portfolio_id, execution_time DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time ON market_data (instrument_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_performance_portfolio_time ON portfolio_performance (portfolio_id, timestamp DESC);

-- Views for current positions
CREATE OR REPLACE VIEW current_positions AS
SELECT DISTINCT ON (portfolio_id, instrument_id)
    portfolio_id,
    instrument_id,
    quantity,
    average_cost,
    market_value,
    unrealized_pnl,
    realized_pnl,
    timestamp
FROM positions
ORDER BY portfolio_id, instrument_id, timestamp DESC;

-- View for portfolio summary
CREATE OR REPLACE VIEW portfolio_summary AS
SELECT 
    p.id as portfolio_id,
    p.name,
    p.base_currency,
    COUNT(cp.instrument_id) as position_count,
    SUM(cp.market_value) as total_market_value,
    SUM(cp.unrealized_pnl) as total_unrealized_pnl,
    SUM(cp.realized_pnl) as total_realized_pnl,
    (SUM(cp.market_value) + SUM(cp.unrealized_pnl)) as net_value
FROM portfolios p
LEFT JOIN current_positions cp ON p.id = cp.portfolio_id
GROUP BY p.id, p.name, p.base_currency;

-- Functions for performance calculations

-- Function to calculate daily returns
CREATE OR REPLACE FUNCTION calculate_daily_return(
    prev_value DECIMAL,
    curr_value DECIMAL
) RETURNS DECIMAL AS $$
BEGIN
    IF prev_value IS NULL OR prev_value = 0 THEN
        RETURN 0;
    END IF;
    RETURN ((curr_value - prev_value) / prev_value);
END;
$$ LANGUAGE plpgsql;

-- Function to update position market values
CREATE OR REPLACE FUNCTION update_position_market_values()
RETURNS TRIGGER AS $$
BEGIN
    -- Update market values for all current positions when new market data arrives
    UPDATE positions 
    SET 
        market_value = quantity * NEW.close_price,
        unrealized_pnl = (quantity * NEW.close_price) - (quantity * average_cost)
    WHERE 
        instrument_id = NEW.instrument_id 
        AND timestamp = (
            SELECT MAX(timestamp) 
            FROM positions 
            WHERE instrument_id = NEW.instrument_id
        );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update position values when market data changes
CREATE TRIGGER trigger_update_position_values
    AFTER INSERT ON market_data
    FOR EACH ROW
    EXECUTE FUNCTION update_position_market_values();

-- Sample data inserts for testing
INSERT INTO portfolios (name, description, base_currency, initial_capital) 
VALUES 
    ('Growth Portfolio', 'Aggressive growth strategy focusing on tech stocks', 'USD', 100000.00),
    ('Conservative Portfolio', 'Conservative balanced portfolio', 'USD', 50000.00)
ON CONFLICT DO NOTHING;

INSERT INTO benchmarks (name, symbol, description)
VALUES 
    ('S&P 500', 'SPY', 'SPDR S&P 500 ETF Trust'),
    ('NASDAQ 100', 'QQQ', 'Invesco QQQ Trust ETF'),
    ('Russell 2000', 'IWM', 'iShares Russell 2000 ETF')
ON CONFLICT DO NOTHING;

-- Sample instruments
INSERT INTO instruments (symbol, instrument_type, exchange, sector, currency)
VALUES 
    ('AAPL', 'stock', 'NASDAQ', 'Technology', 'USD'),
    ('GOOGL', 'stock', 'NASDAQ', 'Technology', 'USD'),
    ('MSFT', 'stock', 'NASDAQ', 'Technology', 'USD'),
    ('TSLA', 'stock', 'NASDAQ', 'Automotive', 'USD'),
    ('SPY', 'etf', 'NYSE', 'Broad Market', 'USD')
ON CONFLICT (symbol) DO NOTHING;