"""
Portfolio Service
Business logic for portfolio management operations
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import asdict
import asyncpg
import pandas as pd
import numpy as np

from ..models.portfolio_models import (
    Portfolio, Position, Transaction, Instrument, MarketData,
    PerformanceMetrics, RiskMetrics, AttributionAnalysis,
    TransactionType, InstrumentType, AttributionType,
    PortfolioCalculator, PerformanceTracker
)


class PortfolioService:
    """
    Core portfolio management service
    Handles position tracking, P&L calculation, and performance analysis
    """
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.logger = logging.getLogger(__name__)
        self.calculator = PortfolioCalculator()
        
    async def create_connection(self) -> asyncpg.Connection:
        """Create database connection"""
        return await asyncpg.connect(self.db_connection_string)
    
    # Portfolio Management
    async def create_portfolio(self, portfolio: Portfolio) -> int:
        """Create a new portfolio"""
        conn = await self.create_connection()
        try:
            portfolio_id = await conn.fetchval(
                """
                INSERT INTO portfolios (name, description, base_currency, initial_capital)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                portfolio.name, portfolio.description, portfolio.base_currency, portfolio.initial_capital
            )
            self.logger.info(f"Created portfolio {portfolio_id}: {portfolio.name}")
            return portfolio_id
        finally:
            await conn.close()
    
    async def get_portfolio(self, portfolio_id: int) -> Optional[Portfolio]:
        """Get portfolio by ID"""
        conn = await self.create_connection()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM portfolios WHERE id = $1",
                portfolio_id
            )
            if row:
                return Portfolio(**dict(row))
            return None
        finally:
            await conn.close()
    
    async def list_portfolios(self) -> List[Portfolio]:
        """List all portfolios"""
        conn = await self.create_connection()
        try:
            rows = await conn.fetch("SELECT * FROM portfolios ORDER BY created_at DESC")
            return [Portfolio(**dict(row)) for row in rows]
        finally:
            await conn.close()
    
    # Instrument Management
    async def create_instrument(self, instrument: Instrument) -> int:
        """Create a new instrument"""
        conn = await self.create_connection()
        try:
            instrument_id = await conn.fetchval(
                """
                INSERT INTO instruments (symbol, instrument_type, exchange, sector, currency, multiplier, tick_size)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol) DO UPDATE SET
                    instrument_type = EXCLUDED.instrument_type,
                    exchange = EXCLUDED.exchange,
                    sector = EXCLUDED.sector,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                instrument.symbol, instrument.instrument_type.value, instrument.exchange,
                instrument.sector, instrument.currency, instrument.multiplier, instrument.tick_size
            )
            self.logger.info(f"Created/updated instrument {instrument_id}: {instrument.symbol}")
            return instrument_id
        finally:
            await conn.close()
    
    async def get_instrument_by_symbol(self, symbol: str) -> Optional[Instrument]:
        """Get instrument by symbol"""
        conn = await self.create_connection()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM instruments WHERE symbol = $1",
                symbol.upper()
            )
            if row:
                return Instrument(**dict(row))
            return None
        finally:
            await conn.close()
    
    # Transaction Management
    async def add_transaction(self, transaction: Transaction) -> int:
        """Add a new transaction and update positions"""
        conn = await self.create_connection()
        try:
            async with conn.transaction():
                # Insert transaction
                transaction_id = await conn.fetchval(
                    """
                    INSERT INTO transactions 
                    (portfolio_id, instrument_id, transaction_type, quantity, price, 
                     total_amount, fees, commission, execution_time, order_id, strategy_name, notes)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    RETURNING id
                    """,
                    transaction.portfolio_id, transaction.instrument_id, transaction.transaction_type.value,
                    transaction.quantity, transaction.price, transaction.total_amount,
                    transaction.fees, transaction.commission, transaction.execution_time,
                    transaction.order_id, transaction.strategy_name, transaction.notes
                )
                
                # Update position
                await self._update_position_from_transaction(conn, transaction)
                
                self.logger.info(f"Added transaction {transaction_id} for portfolio {transaction.portfolio_id}")
                return transaction_id
        finally:
            await conn.close()
    
    async def _update_position_from_transaction(self, conn: asyncpg.Connection, transaction: Transaction):
        """Update position based on transaction"""
        # Get current position
        current_pos = await conn.fetchrow(
            """
            SELECT * FROM positions 
            WHERE portfolio_id = $1 AND instrument_id = $2
            ORDER BY timestamp DESC LIMIT 1
            """,
            transaction.portfolio_id, transaction.instrument_id
        )
        
        if current_pos:
            current_qty = current_pos['quantity']
            current_avg_cost = current_pos['average_cost']
            current_realized_pnl = current_pos['realized_pnl']
        else:
            current_qty = Decimal('0')
            current_avg_cost = Decimal('0')
            current_realized_pnl = Decimal('0')
        
        # Calculate new position based on transaction type
        if transaction.transaction_type in [TransactionType.BUY, TransactionType.SELL]:
            if transaction.transaction_type == TransactionType.BUY:
                new_qty = current_qty + transaction.quantity
                if new_qty != 0:
                    # Weighted average cost
                    total_cost = (current_qty * current_avg_cost) + (transaction.quantity * transaction.price)
                    new_avg_cost = total_cost / new_qty
                else:
                    new_avg_cost = transaction.price
                new_realized_pnl = current_realized_pnl
            else:  # SELL
                new_qty = current_qty - transaction.quantity
                new_avg_cost = current_avg_cost  # Average cost stays the same
                # Calculate realized P&L on sale
                realized_on_sale = transaction.quantity * (transaction.price - current_avg_cost)
                new_realized_pnl = current_realized_pnl + realized_on_sale
        
        elif transaction.transaction_type == TransactionType.DIVIDEND:
            new_qty = current_qty
            new_avg_cost = current_avg_cost
            new_realized_pnl = current_realized_pnl + transaction.total_amount
        
        elif transaction.transaction_type == TransactionType.SPLIT:
            # Quantity adjustment for stock split (transaction.quantity = split ratio)
            new_qty = current_qty * transaction.quantity
            new_avg_cost = current_avg_cost / transaction.quantity if transaction.quantity != 0 else current_avg_cost
            new_realized_pnl = current_realized_pnl
        
        else:
            # Other transaction types (fees, etc.)
            new_qty = current_qty
            new_avg_cost = current_avg_cost
            new_realized_pnl = current_realized_pnl
        
        # Insert new position record
        await conn.execute(
            """
            INSERT INTO positions 
            (timestamp, portfolio_id, instrument_id, quantity, average_cost, 
             market_value, unrealized_pnl, realized_pnl)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            transaction.execution_time, transaction.portfolio_id, transaction.instrument_id,
            new_qty, new_avg_cost, new_qty * transaction.price,
            (new_qty * transaction.price) - (new_qty * new_avg_cost), new_realized_pnl
        )
    
    # Position Management
    async def get_current_positions(self, portfolio_id: int) -> List[Position]:
        """Get current positions for a portfolio"""
        conn = await self.create_connection()
        try:
            rows = await conn.fetch(
                """
                SELECT p.*, i.symbol 
                FROM current_positions p
                JOIN instruments i ON p.instrument_id = i.id
                WHERE p.portfolio_id = $1 AND p.quantity != 0
                ORDER BY p.market_value DESC
                """,
                portfolio_id
            )
            return [Position(**{k: v for k, v in dict(row).items() if k != 'symbol'}) for row in rows]
        finally:
            await conn.close()
    
    async def get_position_history(self, portfolio_id: int, instrument_id: int, 
                                 start_date: Optional[datetime] = None, 
                                 end_date: Optional[datetime] = None) -> List[Position]:
        """Get position history for a specific instrument"""
        conn = await self.create_connection()
        try:
            if start_date is None:
                start_date = datetime.now(timezone.utc) - timedelta(days=30)
            if end_date is None:
                end_date = datetime.now(timezone.utc)
            
            rows = await conn.fetch(
                """
                SELECT * FROM positions 
                WHERE portfolio_id = $1 AND instrument_id = $2 
                AND timestamp BETWEEN $3 AND $4
                ORDER BY timestamp DESC
                """,
                portfolio_id, instrument_id, start_date, end_date
            )
            return [Position(**dict(row)) for row in rows]
        finally:
            await conn.close()
    
    # Market Data Management
    async def update_market_data(self, market_data: List[MarketData]):
        """Update market data for instruments"""
        conn = await self.create_connection()
        try:
            async with conn.transaction():
                for data in market_data:
                    await conn.execute(
                        """
                        INSERT INTO market_data 
                        (timestamp, instrument_id, open_price, high_price, low_price, 
                         close_price, volume, bid_price, ask_price, data_source)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (timestamp, instrument_id) DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume,
                            bid_price = EXCLUDED.bid_price,
                            ask_price = EXCLUDED.ask_price,
                            data_source = EXCLUDED.data_source
                        """,
                        data.timestamp, data.instrument_id, data.open_price, data.high_price,
                        data.low_price, data.close_price, data.volume, data.bid_price,
                        data.ask_price, data.data_source
                    )
                self.logger.info(f"Updated market data for {len(market_data)} instruments")
        finally:
            await conn.close()
    
    async def get_latest_market_data(self, instrument_id: int) -> Optional[MarketData]:
        """Get latest market data for an instrument"""
        conn = await self.create_connection()
        try:
            row = await conn.fetchrow(
                """
                SELECT * FROM market_data 
                WHERE instrument_id = $1 
                ORDER BY timestamp DESC LIMIT 1
                """,
                instrument_id
            )
            if row:
                return MarketData(**dict(row))
            return None
        finally:
            await conn.close()
    
    # Performance Analysis
    async def calculate_portfolio_performance(self, portfolio_id: int, 
                                            start_date: Optional[datetime] = None,
                                            end_date: Optional[datetime] = None) -> PerformanceMetrics:
        """Calculate comprehensive portfolio performance metrics"""
        conn = await self.create_connection()
        try:
            if start_date is None:
                start_date = datetime.now(timezone.utc) - timedelta(days=365)
            if end_date is None:
                end_date = datetime.now(timezone.utc)
            
            # Get portfolio value history
            value_history = await conn.fetch(
                """
                SELECT timestamp, total_value, total_return 
                FROM portfolio_performance 
                WHERE portfolio_id = $1 
                AND timestamp BETWEEN $2 AND $3
                ORDER BY timestamp ASC
                """,
                portfolio_id, start_date, end_date
            )
            
            if not value_history:
                # Calculate current performance if no history exists
                positions = await self.get_current_positions(portfolio_id)
                total_value = sum(pos.market_value or Decimal('0') for pos in positions)
                total_return = sum(pos.total_pnl for pos in positions)
                
                return PerformanceMetrics(
                    timestamp=datetime.now(timezone.utc),
                    portfolio_id=portfolio_id,
                    total_value=total_value,
                    cash_balance=Decimal('0'),
                    invested_amount=sum(pos.quantity * pos.average_cost for pos in positions),
                    total_return=total_return
                )
            
            # Calculate performance metrics from history
            values = [row['total_value'] for row in value_history]
            returns = self.calculator.calculate_returns(values)
            
            if returns:
                latest_metrics = value_history[-1]
                daily_return = returns[-1] if returns else Decimal('0')
                cumulative_return = (values[-1] - values[0]) / values[0] if values[0] != 0 else Decimal('0')
                sharpe_ratio = self.calculator.calculate_sharpe_ratio(returns)
                max_drawdown = self.calculator.calculate_max_drawdown([val / values[0] for val in values])
                volatility = Decimal(str(np.std([float(r) for r in returns]) * np.sqrt(252)))
                
                return PerformanceMetrics(
                    timestamp=latest_metrics['timestamp'],
                    portfolio_id=portfolio_id,
                    total_value=latest_metrics['total_value'],
                    cash_balance=Decimal('0'),
                    invested_amount=Decimal('0'),
                    total_return=latest_metrics['total_return'],
                    daily_return=daily_return,
                    cumulative_return=cumulative_return,
                    sharpe_ratio=sharpe_ratio,
                    max_drawdown=max_drawdown,
                    volatility=volatility
                )
            
            return PerformanceMetrics(
                timestamp=datetime.now(timezone.utc),
                portfolio_id=portfolio_id,
                total_value=Decimal('0'),
                cash_balance=Decimal('0'),
                invested_amount=Decimal('0'),
                total_return=Decimal('0')
            )
        finally:
            await conn.close()
    
    async def update_portfolio_performance(self, portfolio_id: int):
        """Update portfolio performance metrics"""
        conn = await self.create_connection()
        try:
            # Get current positions
            positions = await self.get_current_positions(portfolio_id)
            
            # Calculate current metrics
            total_value = sum(pos.market_value or Decimal('0') for pos in positions)
            invested_amount = sum(pos.quantity * pos.average_cost for pos in positions)
            total_return = sum(pos.total_pnl for pos in positions)
            
            # Get previous day's value for daily return calculation
            prev_value = await conn.fetchval(
                """
                SELECT total_value FROM portfolio_performance 
                WHERE portfolio_id = $1 
                AND timestamp < $2
                ORDER BY timestamp DESC LIMIT 1
                """,
                portfolio_id, datetime.now(timezone.utc) - timedelta(hours=1)
            )
            
            daily_return = None
            if prev_value and prev_value != 0:
                daily_return = (total_value - prev_value) / prev_value
            
            # Insert performance record
            await conn.execute(
                """
                INSERT INTO portfolio_performance 
                (timestamp, portfolio_id, total_value, cash_balance, invested_amount, 
                 total_return, daily_return)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                datetime.now(timezone.utc), portfolio_id, total_value, Decimal('0'),
                invested_amount, total_return, daily_return
            )
            
            self.logger.info(f"Updated performance for portfolio {portfolio_id}")
        finally:
            await conn.close()
    
    # Risk Analysis
    async def calculate_risk_metrics(self, portfolio_id: int, 
                                   lookback_days: int = 252) -> RiskMetrics:
        """Calculate comprehensive risk metrics"""
        conn = await self.create_connection()
        try:
            # Get returns history
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=lookback_days)
            
            returns_data = await conn.fetch(
                """
                SELECT daily_return FROM portfolio_performance 
                WHERE portfolio_id = $1 
                AND timestamp BETWEEN $2 AND $3
                AND daily_return IS NOT NULL
                ORDER BY timestamp ASC
                """,
                portfolio_id, start_date, end_date
            )
            
            if not returns_data:
                return RiskMetrics(
                    timestamp=datetime.now(timezone.utc),
                    portfolio_id=portfolio_id
                )
            
            returns = [row['daily_return'] for row in returns_data]
            
            # Calculate VaR and CVaR
            var_95 = self.calculator.calculate_var(returns, 0.95)
            var_99 = self.calculator.calculate_var(returns, 0.99)
            
            # Calculate CVaR (Expected Shortfall)
            returns_array = np.array([float(r) for r in returns])
            var_95_value = float(var_95)
            var_99_value = float(var_99)
            
            cvar_95 = Decimal(str(np.mean(returns_array[returns_array <= var_95_value]))) if var_95_value < 0 else Decimal('0')
            cvar_99 = Decimal(str(np.mean(returns_array[returns_array <= var_99_value]))) if var_99_value < 0 else Decimal('0')
            
            # Calculate Sortino ratio (downside deviation)
            downside_returns = [r for r in returns if r < 0]
            if downside_returns:
                downside_vol = Decimal(str(np.std([float(r) for r in downside_returns]) * np.sqrt(252)))
                mean_return = Decimal(str(np.mean([float(r) for r in returns]) * 252))
                sortino_ratio = mean_return / downside_vol if downside_vol != 0 else Decimal('0')
            else:
                sortino_ratio = Decimal('0')
            
            # Get current portfolio value for drawdown calculation
            value_history = await conn.fetch(
                """
                SELECT total_value FROM portfolio_performance 
                WHERE portfolio_id = $1 
                AND timestamp BETWEEN $2 AND $3
                ORDER BY timestamp ASC
                """,
                portfolio_id, start_date, end_date
            )
            
            max_drawdown = Decimal('0')
            if value_history and len(value_history) > 1:
                values = [row['total_value'] for row in value_history]
                normalized_values = [val / values[0] for val in values]
                max_drawdown = self.calculator.calculate_max_drawdown(normalized_values)
            
            return RiskMetrics(
                timestamp=datetime.now(timezone.utc),
                portfolio_id=portfolio_id,
                var_95=var_95,
                var_99=var_99,
                cvar_95=cvar_95,
                cvar_99=cvar_99,
                sortino_ratio=sortino_ratio,
                maximum_drawdown=max_drawdown
            )
        finally:
            await conn.close()
    
    # Attribution Analysis
    async def calculate_attribution_analysis(self, portfolio_id: int, 
                                           benchmark_id: Optional[int] = None) -> List[AttributionAnalysis]:
        """Calculate performance attribution analysis"""
        conn = await self.create_connection()
        try:
            # Get current positions with sector information
            positions_data = await conn.fetch(
                """
                SELECT p.*, i.symbol, i.sector, i.instrument_type
                FROM current_positions p
                JOIN instruments i ON p.instrument_id = i.id
                WHERE p.portfolio_id = $1 AND p.quantity != 0
                """,
                portfolio_id
            )
            
            if not positions_data:
                return []
            
            # Calculate total portfolio value
            total_value = sum(row['market_value'] for row in positions_data)
            
            attributions = []
            timestamp = datetime.now(timezone.utc)
            
            # Stock selection attribution
            for row in positions_data:
                weight = row['market_value'] / total_value if total_value != 0 else Decimal('0')
                return_contrib = row['unrealized_pnl'] / total_value if total_value != 0 else Decimal('0')
                
                attributions.append(AttributionAnalysis(
                    timestamp=timestamp,
                    portfolio_id=portfolio_id,
                    instrument_id=row['instrument_id'],
                    attribution_type=AttributionType.STOCK_SELECTION,
                    contribution=return_contrib,
                    weight=weight
                ))
            
            # Sector allocation attribution
            sector_contributions = {}
            for row in positions_data:
                sector = row['sector'] or 'Unknown'
                if sector not in sector_contributions:
                    sector_contributions[sector] = {
                        'value': Decimal('0'),
                        'pnl': Decimal('0')
                    }
                sector_contributions[sector]['value'] += row['market_value']
                sector_contributions[sector]['pnl'] += row['unrealized_pnl']
            
            for sector, data in sector_contributions.items():
                weight = data['value'] / total_value if total_value != 0 else Decimal('0')
                contribution = data['pnl'] / total_value if total_value != 0 else Decimal('0')
                
                attributions.append(AttributionAnalysis(
                    timestamp=timestamp,
                    portfolio_id=portfolio_id,
                    sector=sector,
                    attribution_type=AttributionType.SECTOR_ALLOCATION,
                    contribution=contribution,
                    weight=weight
                ))
            
            return attributions
        finally:
            await conn.close()
    
    # Real-time Position Updates
    async def update_positions_with_market_data(self, portfolio_id: Optional[int] = None):
        """Update all positions with latest market data"""
        conn = await self.create_connection()
        try:
            query = """
                UPDATE positions 
                SET 
                    market_value = quantity * md.close_price,
                    unrealized_pnl = (quantity * md.close_price) - (quantity * average_cost)
                FROM market_data md
                WHERE positions.instrument_id = md.instrument_id
                AND md.timestamp = (
                    SELECT MAX(timestamp) 
                    FROM market_data 
                    WHERE instrument_id = positions.instrument_id
                )
                AND positions.timestamp = (
                    SELECT MAX(timestamp) 
                    FROM positions p2 
                    WHERE p2.portfolio_id = positions.portfolio_id 
                    AND p2.instrument_id = positions.instrument_id
                )
            """
            
            if portfolio_id:
                query += " AND positions.portfolio_id = $1"
                await conn.execute(query, portfolio_id)
            else:
                await conn.execute(query)
            
            self.logger.info(f"Updated positions with latest market data")
        finally:
            await conn.close()
    
    # Portfolio Summary
    async def get_portfolio_summary(self, portfolio_id: int) -> Dict[str, Any]:
        """Get comprehensive portfolio summary"""
        conn = await self.create_connection()
        try:
            # Get basic portfolio info
            portfolio = await self.get_portfolio(portfolio_id)
            if not portfolio:
                return {}
            
            # Get current positions
            positions = await self.get_current_positions(portfolio_id)
            
            # Get performance metrics
            performance = await self.calculate_portfolio_performance(portfolio_id)
            
            # Get risk metrics
            risk = await self.calculate_risk_metrics(portfolio_id)
            
            # Calculate summary statistics
            total_value = sum(pos.market_value or Decimal('0') for pos in positions)
            total_pnl = sum(pos.total_pnl for pos in positions)
            total_return_pct = (total_pnl / portfolio.initial_capital * 100) if portfolio.initial_capital != 0 else Decimal('0')
            
            return {
                'portfolio': asdict(portfolio),
                'summary': {
                    'position_count': len(positions),
                    'total_value': total_value,
                    'total_pnl': total_pnl,
                    'total_return_pct': total_return_pct,
                    'cash_balance': Decimal('0')  # Placeholder for cash tracking
                },
                'performance': asdict(performance),
                'risk': asdict(risk),
                'positions': [asdict(pos) for pos in positions[:10]]  # Top 10 positions
            }
        finally:
            await conn.close()