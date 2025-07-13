# Bespin Trading Platform

A comprehensive quantitative trading platform built on FastAPI with PostgreSQL+TimescaleDB, Redis, and AsyncIO for high-performance trading strategy execution.

## 🚀 Features

### Core Trading Engine
- **Strategy Execution Framework** - Signal generation and automated order execution
- **Order Management System** - Complete order lifecycle with broker integration (Alpaca)
- **Position Tracking** - Real-time P&L calculation and position management
- **Real-time Market Data** - WebSocket streaming and historical data access
- **Risk Management** - Stop-loss, take-profit, and position sizing controls

### API & Integration
- **FastAPI REST API** - Full CRUD operations for strategies, orders, and positions
- **WebSocket Streaming** - Real-time market data and trading updates
- **Database Integration** - PostgreSQL + TimescaleDB for time-series optimization
- **Existing Infrastructure** - Seamlessly integrates with aop.py data orchestrator

### Technology Stack
- **Backend**: FastAPI, AsyncIO, SQLAlchemy
- **Database**: PostgreSQL + TimescaleDB, Redis
- **Broker**: Alpaca Markets integration
- **Data Sources**: Integration with 15+ market data providers via aop.py

## 📋 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI REST API                        │
├─────────────────────────────────────────────────────────────┤
│  Strategies │  Orders  │ Positions │  Market Data │ Health  │
├─────────────────────────────────────────────────────────────┤
│                   Trading Engine                           │
├─────────────────────────────────────────────────────────────┤
│ Strategy    │  Order    │ Position  │  Market Data │ Risk   │
│ Executor    │ Manager   │ Manager   │   Handler    │ Mgmt   │
├─────────────────────────────────────────────────────────────┤
│              Data Layer & Integration                       │
├─────────────────────────────────────────────────────────────┤
│ PostgreSQL  │   Redis   │ Alpaca    │   aop.py     │ News   │
│TimescaleDB  │           │ Broker    │ Orchestrator │Engines │
└─────────────────────────────────────────────────────────────┘
```

## 🛠 Installation

### Prerequisites
- Python 3.9+
- PostgreSQL 14+ with TimescaleDB extension
- Redis 6+
- Environment variables for broker access

### Setup

1. **Clone and install dependencies:**
```bash
cd /workspaces/bespin
pip install -r trading_platform/requirements.txt
```

2. **Database setup:**
```sql
-- Connect to PostgreSQL and run:
CREATE DATABASE trading_platform;
\c trading_platform;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Run schema creation:
\i trading_platform/database/schemas.sql
```

3. **Environment variables:**
```bash
# Create .env file with:
ALPACA_API_KEY=your_alpaca_api_key
ALPACA_SEC_KEY=your_alpaca_secret_key
DATABASE_URL=postgresql://user:pass@localhost/trading_platform
REDIS_URL=redis://localhost:6379
```

4. **Start the platform:**
```bash
python -m trading_platform.main --host 0.0.0.0 --port 8000 --simulation
```

## 🚀 Quick Start

### 1. Start the Server
```bash
python -m trading_platform.main --simulation
```

### 2. Access the API
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **WebSocket**: ws://localhost:8000/api/v1/market-data/stream

### 3. Create a Strategy
```bash
curl -X POST "http://localhost:8000/api/v1/strategies" \
-H "Content-Type: application/json" \
-d '{
  "name": "Simple Momentum",
  "strategy_type": "simple_momentum", 
  "symbols": ["AAPL", "GOOGL"],
  "parameters": {"threshold": 0.02}
}'
```

### 4. Place an Order
```bash
curl -X POST "http://localhost:8000/api/v1/orders" \
-H "Content-Type: application/json" \
-d '{
  "symbol": "AAPL",
  "side": "buy",
  "quantity": 100,
  "order_type": "market"
}'
```

### 5. Monitor Positions
```bash
curl "http://localhost:8000/api/v1/positions"
```

## 📊 API Endpoints

### Strategies (`/api/v1/strategies`)
- `GET /` - List all strategies
- `POST /` - Create new strategy
- `GET /{id}` - Get strategy details
- `POST /{id}/start` - Start strategy
- `POST /{id}/stop` - Stop strategy
- `GET /{id}/performance` - Strategy performance metrics

### Orders (`/api/v1/orders`)
- `GET /` - List orders with filtering
- `POST /` - Create new order
- `GET /{id}` - Get order details
- `POST /{id}/cancel` - Cancel order
- `GET /pending` - Get pending orders
- `POST /bulk/cancel` - Bulk cancel orders

### Positions (`/api/v1/positions`)
- `GET /` - List positions
- `POST /` - Create manual position
- `GET /{id}` - Get position details
- `POST /{id}/close` - Close position
- `GET /open` - Get open positions
- `GET /pnl/summary` - P&L summary

### Market Data (`/api/v1/market-data`)
- `GET /quote/{symbol}` - Latest quote
- `GET /quotes?symbols=AAPL,GOOGL` - Multiple quotes
- `GET /historical/{symbol}` - Historical data
- `POST /subscribe` - Subscribe to symbols
- `WebSocket /stream` - Real-time streaming

## 🔧 Configuration

### Simulation Mode
The platform starts in simulation mode by default for safe testing:
```bash
python -m trading_platform.main --simulation
```

### Production Mode
For live trading (requires valid broker credentials):
```bash
python -m trading_platform.main --host 0.0.0.0 --port 8000
```

### Integration with aop.py
The platform automatically integrates with the existing aop.py data orchestrator to leverage:
- 15+ market data sources (Alpaca, Polygon, Alpha Vantage, etc.)
- News engines (Barrons, Forbes, Benzinga, etc.)
- Economic data (FRED)
- Real-time data feeds

## 📈 Strategy Development

### Built-in Strategies
- **Simple Momentum**: Buy on upward price movement
- **Mean Reversion**: Buy low, sell high based on price ranges
- **Custom**: Implement your own strategy logic

### Strategy Example
```python
# Custom strategy implementation
class CustomStrategy:
    def __init__(self, symbols, parameters):
        self.symbols = symbols
        self.parameters = parameters
    
    async def generate_signal(self, symbol, market_data):
        current_price = market_data['price']
        
        # Your strategy logic here
        if current_price > self.parameters['buy_threshold']:
            return {
                'action': 'BUY',
                'quantity': 100,
                'confidence': 0.8
            }
        
        return {'action': 'HOLD'}
```

## 🗄 Database Schema

### Core Tables
- **strategies** - Strategy configurations
- **orders** - Order lifecycle tracking
- **positions** - Position management and P&L
- **market_data** - Real-time market data (TimescaleDB hypertable)
- **ohlcv_bars** - OHLCV candlestick data (TimescaleDB hypertable)

### Time-Series Optimization
- TimescaleDB hypertables for market data
- Automatic data retention policies
- Continuous aggregates for performance
- Automated performance metric calculations

## 🔒 Security & Risk Management

### Risk Controls
- Position sizing limits
- Stop-loss and take-profit orders
- Maximum drawdown limits
- Daily loss limits

### Authentication
- API key authentication (placeholder)
- Permission-based access control
- Rate limiting (configurable)

## 📊 Performance & Monitoring

### Metrics Tracking
- Order execution latency
- Strategy performance
- P&L tracking
- System health monitoring

### Logging
- Structured logging with levels
- Performance metrics
- Error tracking
- Audit trail

## 🧪 Testing

### Run Tests
```bash
pytest trading_platform/tests/
```

### Test Coverage
```bash
pytest --cov=trading_platform trading_platform/tests/
```

## 📝 Development

### Code Style
```bash
black trading_platform/
flake8 trading_platform/
mypy trading_platform/
```

### Development Server
```bash
python -m trading_platform.main --reload --log-level DEBUG
```

## 🤝 Integration Points

### Existing Bespin Infrastructure
- **aop.py Integration**: Leverages existing data orchestrator
- **Data Engines**: Reuses 15+ market data sources
- **News Processing**: Integrates with news extraction engines
- **Database**: Extends existing database infrastructure

### Broker Integration
- **Alpaca Markets**: Primary broker integration
- **Extensible**: Easy to add other brokers
- **Simulation Mode**: Safe testing environment

## 📚 Documentation

- **API Docs**: Available at `/docs` when server is running
- **Database Schema**: See `database/schemas.sql`
- **Architecture**: Detailed in code comments

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Ensure PostgreSQL is running
   - Check DATABASE_URL environment variable
   - Verify TimescaleDB extension is installed

2. **Broker Connection Error**
   - Verify ALPACA_API_KEY and ALPACA_SEC_KEY
   - Check network connectivity
   - Ensure broker account is active

3. **Import Errors**
   - Verify all dependencies are installed
   - Check Python path configuration
   - Ensure trading_platform is in PYTHONPATH

## 📄 License

This trading platform is part of the Bespin financial data platform.

## 🆘 Support

For issues and questions:
1. Check the API documentation at `/docs`
2. Review the health check endpoint at `/health`
3. Check logs in `trading_platform.log`
4. Verify environment variables and dependencies