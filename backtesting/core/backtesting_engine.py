"""
Comprehensive Backtesting Engine
Core engine that integrates with existing trading platform and portfolio system
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any, Tuple, Set
import asyncpg
import pandas as pd
import numpy as np
from dataclasses import replace
import json

# Import existing trading platform components
import sys
import os
sys.path.append('/workspaces/bespin')

from trading_platform.models.strategies import Strategy, StrategySignal
from trading_platform.core.market_data_handler import MarketDataHandler
from portfolio.models.portfolio_models import Portfolio, Position, Transaction, PerformanceMetrics
from portfolio.services.performance_engine import PerformanceEngine

# Import backtesting models
from ..models.backtesting_models import (
    BacktestConfig, BacktestResults, BacktestSnapshot, BacktestPosition, 
    BacktestTrade, BacktestStatus, BacktestMode
)


class BacktestingEngine:
    """
    Comprehensive backtesting engine that integrates with existing systems
    """
    
    def __init__(self, db_connection_string: str, redis_url: str = "redis://localhost:6379"):
        self.db_connection_string = db_connection_string
        self.redis_url = redis_url
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.market_data_handler = None
        self.performance_engine = None
        
        # Backtesting state
        self.current_backtest: Optional[BacktestResults] = None
        self.running_backtests: Dict[str, BacktestResults] = {}
        
    async def initialize(self):
        """Initialize the backtesting engine"""
        try:
            # Initialize market data handler (reuse existing)
            self.market_data_handler = MarketDataHandler(
                db_connection_string=self.db_connection_string,
                redis_url=self.redis_url
            )
            await self.market_data_handler.initialize()
            
            # Initialize performance engine (reuse existing)
            self.performance_engine = PerformanceEngine(self.db_connection_string)
            
            self.logger.info("Backtesting engine initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize backtesting engine: {str(e)}")
            raise
    
    async def run_backtest(self, config: BacktestConfig, strategy: Strategy) -> BacktestResults:
        """
        Run a comprehensive backtest
        """
        backtest_id = config.backtest_id
        self.logger.info(f"Starting backtest {backtest_id}")
        
        # Create results container
        results = BacktestResults(
            config=config,
            status=BacktestStatus.RUNNING,
            start_time=datetime.now(timezone.utc)
        )
        
        self.running_backtests[backtest_id] = results
        self.current_backtest = results
        
        try:
            # Execute based on mode
            if config.mode == BacktestMode.FULL:
                await self._run_full_backtest(results, strategy)
            elif config.mode == BacktestMode.WALK_FORWARD:
                await self._run_walk_forward_analysis(results, strategy)
            elif config.mode == BacktestMode.MONTE_CARLO:
                await self._run_monte_carlo_simulation(results, strategy)
            elif config.mode == BacktestMode.STRESS_TEST:
                await self._run_stress_test_analysis(results, strategy)
            
            # Finalize results
            results.status = BacktestStatus.COMPLETED
            results.end_time = datetime.now(timezone.utc)
            if results.start_time:
                results.execution_duration = (results.end_time - results.start_time).total_seconds()
            
            # Calculate final metrics
            results.calculate_summary_metrics()
            
            # Calculate benchmark comparison if specified
            if config.benchmark_symbol:
                await self._calculate_benchmark_comparison(results)
            
            self.logger.info(f"Backtest {backtest_id} completed successfully")
            
        except Exception as e:
            results.status = BacktestStatus.FAILED
            results.error_message = str(e)
            results.end_time = datetime.now(timezone.utc)
            self.logger.error(f"Backtest {backtest_id} failed: {str(e)}")
            raise
        
        finally:
            if backtest_id in self.running_backtests:
                del self.running_backtests[backtest_id]
        
        return results
    
    async def _run_full_backtest(self, results: BacktestResults, strategy: Strategy):
        """Run a full historical backtest"""
        config = results.config
        
        # Initialize portfolio state
        cash = config.initial_capital
        positions: Dict[str, BacktestPosition] = {}
        portfolio_value = cash
        
        # Get historical data for universe
        historical_data = await self._load_historical_data(
            config.universe, config.start_date, config.end_date
        )
        
        if not historical_data:
            raise ValueError("No historical data available for the specified period")
        
        # Get trading calendar
        trading_dates = self._get_trading_dates(config.start_date, config.end_date)
        
        self.logger.info(f"Running backtest over {len(trading_dates)} trading days")
        
        # Main backtesting loop
        for i, current_date in enumerate(trading_dates):
            try:
                # Update progress
                progress = (i / len(trading_dates)) * 100
                
                # Get market data for current date
                market_data = self._get_market_data_for_date(historical_data, current_date)
                if not market_data:
                    continue
                
                # Update position values
                cash, positions, portfolio_value = await self._update_positions(
                    cash, positions, market_data, current_date
                )
                
                # Generate strategy signals
                signals = await self._generate_strategy_signals(
                    strategy, market_data, current_date, config.universe
                )
                
                # Execute trades based on signals
                cash, positions, new_trades = await self._execute_trades(
                    cash, positions, signals, market_data, current_date, config
                )
                
                # Record trades
                results.trades.extend(new_trades)
                
                # Calculate daily metrics
                daily_return = Decimal("0")
                if i > 0 and results.snapshots:
                    prev_value = results.snapshots[-1].total_value
                    if prev_value > 0:
                        daily_return = (portfolio_value - prev_value) / prev_value
                
                # Create portfolio snapshot
                snapshot = BacktestSnapshot(
                    timestamp=current_date,
                    total_value=portfolio_value,
                    cash=cash,
                    invested_value=sum(pos.market_value for pos in positions.values()),
                    positions=list(positions.values()),
                    daily_return=daily_return
                )
                
                # Calculate cumulative return
                if config.initial_capital > 0:
                    snapshot.cumulative_return = (portfolio_value - config.initial_capital) / config.initial_capital
                
                results.snapshots.append(snapshot)
                
                # Risk management checks
                await self._apply_risk_management(cash, positions, config, current_date)
                
            except Exception as e:
                self.logger.warning(f"Error processing date {current_date}: {str(e)}")
                continue
        
        self.logger.info(f"Backtest completed with {len(results.trades)} trades")
    
    async def _run_walk_forward_analysis(self, results: BacktestResults, strategy: Strategy):
        """Run walk-forward analysis"""
        config = results.config
        
        if not config.training_window or not config.testing_window or not config.step_size:
            raise ValueError("Walk-forward analysis requires training_window, testing_window, and step_size")
        
        walk_forward_results = []
        
        # Calculate walk-forward periods
        current_start = config.start_date
        
        while current_start + timedelta(days=config.training_window + config.testing_window) <= config.end_date:
            # Define training and testing periods
            training_end = current_start + timedelta(days=config.training_window)
            testing_start = training_end
            testing_end = testing_start + timedelta(days=config.testing_window)
            
            self.logger.info(f"Walk-forward period: train {current_start} to {training_end}, test {testing_start} to {testing_end}")
            
            try:
                # Create sub-backtest for this period
                sub_config = replace(config,
                    start_date=testing_start,
                    end_date=testing_end,
                    mode=BacktestMode.FULL
                )
                
                # Run optimization on training period (placeholder for now)
                optimized_strategy = await self._optimize_strategy_parameters(
                    strategy, current_start, training_end, config.universe
                )
                
                # Run backtest on testing period
                sub_results = BacktestResults(config=sub_config)
                await self._run_full_backtest(sub_results, optimized_strategy)
                
                # Store results
                walk_forward_result = {
                    "training_start": current_start,
                    "training_end": training_end,
                    "testing_start": testing_start,
                    "testing_end": testing_end,
                    "total_return": sub_results.total_return,
                    "sharpe_ratio": sub_results.sharpe_ratio,
                    "max_drawdown": sub_results.max_drawdown,
                    "num_trades": len(sub_results.trades),
                    "win_rate": sub_results.win_rate
                }
                walk_forward_results.append(walk_forward_result)
                
                # Add trades and snapshots to main results
                results.trades.extend(sub_results.trades)
                results.snapshots.extend(sub_results.snapshots)
                
            except Exception as e:
                self.logger.warning(f"Walk-forward period failed: {str(e)}")
                continue
            
            # Move to next period
            current_start += timedelta(days=config.step_size)
        
        results.walk_forward_results = walk_forward_results
        self.logger.info(f"Walk-forward analysis completed with {len(walk_forward_results)} periods")
    
    async def _run_monte_carlo_simulation(self, results: BacktestResults, strategy: Strategy):
        """Run Monte Carlo simulation"""
        config = results.config
        
        if not config.num_simulations:
            raise ValueError("Monte Carlo simulation requires num_simulations")
        
        simulation_results = []
        
        for sim in range(config.num_simulations):
            try:
                self.logger.info(f"Running Monte Carlo simulation {sim + 1}/{config.num_simulations}")
                
                # Create randomized market data (simplified approach)
                # In practice, this would use more sophisticated bootstrapping
                randomized_config = replace(config, mode=BacktestMode.FULL)
                
                # Run simulation
                sim_results = BacktestResults(config=randomized_config)
                await self._run_full_backtest(sim_results, strategy)
                
                # Store key metrics
                simulation_result = {
                    "simulation": sim + 1,
                    "total_return": sim_results.total_return,
                    "sharpe_ratio": sim_results.sharpe_ratio,
                    "max_drawdown": sim_results.max_drawdown,
                    "final_value": sim_results.snapshots[-1].total_value if sim_results.snapshots else config.initial_capital
                }
                simulation_results.append(simulation_result)
                
            except Exception as e:
                self.logger.warning(f"Monte Carlo simulation {sim + 1} failed: {str(e)}")
                continue
        
        # Calculate Monte Carlo statistics
        if simulation_results:
            returns = [sim["total_return"] for sim in simulation_results]
            sharpe_ratios = [sim["sharpe_ratio"] for sim in simulation_results]
            drawdowns = [sim["max_drawdown"] for sim in simulation_results]
            
            monte_carlo_stats = {
                "num_simulations": len(simulation_results),
                "return_statistics": {
                    "mean": float(np.mean([float(r) for r in returns])),
                    "std": float(np.std([float(r) for r in returns])),
                    "min": float(np.min([float(r) for r in returns])),
                    "max": float(np.max([float(r) for r in returns])),
                    "percentiles": {
                        "p5": float(np.percentile([float(r) for r in returns], 5)),
                        "p25": float(np.percentile([float(r) for r in returns], 25)),
                        "p50": float(np.percentile([float(r) for r in returns], 50)),
                        "p75": float(np.percentile([float(r) for r in returns], 75)),
                        "p95": float(np.percentile([float(r) for r in returns], 95))
                    }
                },
                "sharpe_statistics": {
                    "mean": float(np.mean([float(s) for s in sharpe_ratios])),
                    "std": float(np.std([float(s) for s in sharpe_ratios]))
                },
                "drawdown_statistics": {
                    "mean": float(np.mean([float(d) for d in drawdowns])),
                    "max": float(np.max([float(d) for d in drawdowns]))
                },
                "simulation_results": simulation_results
            }
            
            results.monte_carlo_results = monte_carlo_stats
        
        self.logger.info(f"Monte Carlo simulation completed with {len(simulation_results)} successful runs")
    
    async def _run_stress_test_analysis(self, results: BacktestResults, strategy: Strategy):
        """Run stress test scenarios"""
        config = results.config
        
        # Define stress test scenarios
        scenarios = [
            {"name": "2008_financial_crisis", "market_shock": -0.4, "volatility_shock": 2.0},
            {"name": "covid_crash", "market_shock": -0.35, "volatility_shock": 1.8},
            {"name": "flash_crash", "market_shock": -0.1, "volatility_shock": 3.0},
            {"name": "high_inflation", "market_shock": -0.15, "volatility_shock": 1.5},
            {"name": "rising_rates", "market_shock": -0.2, "volatility_shock": 1.3}
        ]
        
        stress_test_results = []
        
        for scenario in scenarios:
            try:
                self.logger.info(f"Running stress test: {scenario['name']}")
                
                # Apply stress scenario to historical data
                # This is a simplified implementation
                stressed_config = replace(config, mode=BacktestMode.FULL)
                
                # Run backtest with stressed data
                stress_results = BacktestResults(config=stressed_config)
                await self._run_full_backtest(stress_results, strategy)
                
                # Calculate stress impact
                baseline_return = results.total_return if results.snapshots else Decimal("0")
                stress_impact = stress_results.total_return - baseline_return
                
                stress_result = {
                    "scenario": scenario['name'],
                    "market_shock": scenario['market_shock'],
                    "volatility_shock": scenario['volatility_shock'],
                    "stressed_return": stress_results.total_return,
                    "stress_impact": stress_impact,
                    "stressed_sharpe": stress_results.sharpe_ratio,
                    "stressed_max_drawdown": stress_results.max_drawdown
                }
                stress_test_results.append(stress_result)
                
            except Exception as e:
                self.logger.warning(f"Stress test {scenario['name']} failed: {str(e)}")
                continue
        
        # Store stress test results in the main results
        if not results.monte_carlo_results:
            results.monte_carlo_results = {}
        results.monte_carlo_results["stress_tests"] = stress_test_results
        
        self.logger.info(f"Stress testing completed with {len(stress_test_results)} scenarios")
    
    async def _load_historical_data(self, universe: List[str], start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Load historical market data for the universe"""
        historical_data = {}
        
        conn = await asyncpg.connect(self.db_connection_string)
        try:
            for symbol in universe:
                # Query historical OHLCV data from TimescaleDB
                rows = await conn.fetch(
                    """
                    SELECT timestamp, open_price, high_price, low_price, close_price, volume
                    FROM ohlcv_bars 
                    WHERE symbol = $1 
                    AND timestamp BETWEEN $2 AND $3
                    ORDER BY timestamp ASC
                    """,
                    symbol, start_date, end_date
                )
                
                if rows:
                    df = pd.DataFrame([dict(row) for row in rows])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    historical_data[symbol] = df
                else:
                    self.logger.warning(f"No historical data found for {symbol}")
            
        finally:
            await conn.close()
        
        return historical_data
    
    def _get_trading_dates(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Get list of trading dates (simplified - excludes weekends)"""
        dates = []
        current = start_date
        
        while current <= end_date:
            # Simple check for weekdays (real implementation would use trading calendar)
            if current.weekday() < 5:  # Monday=0, Friday=4
                dates.append(current)
            current += timedelta(days=1)
        
        return dates
    
    def _get_market_data_for_date(self, historical_data: Dict[str, pd.DataFrame], date: datetime) -> Dict[str, Dict[str, float]]:
        """Get market data for all symbols on a specific date"""
        market_data = {}
        
        for symbol, df in historical_data.items():
            try:
                # Get data for the specific date
                date_str = date.strftime('%Y-%m-%d')
                if date_str in df.index:
                    row = df.loc[date_str]
                    market_data[symbol] = {
                        'open': float(row['open_price']),
                        'high': float(row['high_price']),
                        'low': float(row['low_price']),
                        'close': float(row['close_price']),
                        'volume': float(row['volume']) if row['volume'] else 0.0
                    }
            except Exception as e:
                self.logger.debug(f"No data for {symbol} on {date}: {str(e)}")
                continue
        
        return market_data
    
    async def _update_positions(self, cash: Decimal, positions: Dict[str, BacktestPosition], 
                              market_data: Dict[str, Dict[str, float]], current_date: datetime) -> Tuple[Decimal, Dict[str, BacktestPosition], Decimal]:
        """Update position values based on current market data"""
        updated_positions = {}
        total_invested = Decimal("0")
        
        for symbol, position in positions.items():
            if symbol in market_data:
                # Update position with current price
                current_price = Decimal(str(market_data[symbol]['close']))
                updated_position = replace(position, 
                    current_price=current_price,
                    timestamp=current_date
                )
                updated_positions[symbol] = updated_position
                total_invested += updated_position.market_value
            else:
                # Keep position with last known price
                updated_positions[symbol] = position
                total_invested += position.market_value
        
        portfolio_value = cash + total_invested
        return cash, updated_positions, portfolio_value
    
    async def _generate_strategy_signals(self, strategy: Strategy, market_data: Dict[str, Dict[str, float]], 
                                       current_date: datetime, universe: List[str]) -> List[Dict[str, Any]]:
        """Generate trading signals from strategy"""
        signals = []
        
        # This is a simplified implementation
        # In practice, this would integrate with the existing strategy framework
        
        for symbol in universe:
            if symbol in market_data:
                # Placeholder for strategy signal generation
                # Real implementation would call strategy.generate_signal()
                
                # Simple momentum strategy example
                if 'close' in market_data[symbol]:
                    price = market_data[symbol]['close']
                    
                    # Generate random signal for demonstration
                    # Replace with actual strategy logic
                    import random
                    if random.random() > 0.95:  # 5% chance of signal
                        signal_type = random.choice(['BUY', 'SELL'])
                        signals.append({
                            'symbol': symbol,
                            'signal': signal_type,
                            'confidence': random.uniform(0.6, 1.0),
                            'price': price,
                            'timestamp': current_date
                        })
        
        return signals
    
    async def _execute_trades(self, cash: Decimal, positions: Dict[str, BacktestPosition], 
                            signals: List[Dict[str, Any]], market_data: Dict[str, Dict[str, float]],
                            current_date: datetime, config: BacktestConfig) -> Tuple[Decimal, Dict[str, BacktestPosition], List[BacktestTrade]]:
        """Execute trades based on signals"""
        updated_cash = cash
        updated_positions = positions.copy()
        new_trades = []
        
        for signal in signals:
            symbol = signal['symbol']
            signal_type = signal['signal']
            price = Decimal(str(signal['price']))
            confidence = Decimal(str(signal['confidence']))
            
            try:
                if signal_type == 'BUY':
                    # Calculate position size
                    position_value = self._calculate_position_size(updated_cash, price, config)
                    if position_value > 0:
                        quantity = position_value / price
                        
                        # Apply transaction costs
                        commission = self._calculate_commission(position_value, config)
                        slippage = self._calculate_slippage(position_value, config)
                        total_cost = position_value + commission + slippage
                        
                        if total_cost <= updated_cash:
                            # Execute buy
                            updated_cash -= total_cost
                            
                            if symbol in updated_positions:
                                # Add to existing position (simplified)
                                existing_pos = updated_positions[symbol]
                                new_avg_cost = ((existing_pos.quantity * existing_pos.entry_price) + 
                                              (quantity * price)) / (existing_pos.quantity + quantity)
                                updated_positions[symbol] = replace(existing_pos,
                                    quantity=existing_pos.quantity + quantity,
                                    entry_price=new_avg_cost,
                                    current_price=price,
                                    timestamp=current_date
                                )
                            else:
                                # Create new position
                                updated_positions[symbol] = BacktestPosition(
                                    timestamp=current_date,
                                    symbol=symbol,
                                    quantity=quantity,
                                    entry_price=price,
                                    current_price=price,
                                    entry_date=current_date,
                                    strategy_name=config.name
                                )
                            
                            # Record trade
                            trade = BacktestTrade(
                                symbol=symbol,
                                strategy_name=config.name,
                                entry_date=current_date,
                                entry_price=price,
                                entry_quantity=quantity,
                                entry_commission=commission,
                                entry_slippage=slippage,
                                side='long',
                                confidence=confidence
                            )
                            new_trades.append(trade)
                
                elif signal_type == 'SELL' and symbol in updated_positions:
                    # Sell existing position
                    position = updated_positions[symbol]
                    sell_quantity = position.quantity  # Sell entire position for simplicity
                    
                    if sell_quantity > 0:
                        sale_value = sell_quantity * price
                        commission = self._calculate_commission(sale_value, config)
                        slippage = self._calculate_slippage(sale_value, config)
                        net_proceeds = sale_value - commission - slippage
                        
                        # Update cash
                        updated_cash += net_proceeds
                        
                        # Remove position
                        del updated_positions[symbol]
                        
                        # Record completed trade (find corresponding buy trade and close it)
                        trade = BacktestTrade(
                            symbol=symbol,
                            strategy_name=config.name,
                            entry_date=position.entry_date,
                            entry_price=position.entry_price,
                            entry_quantity=position.quantity,
                            side='long'
                        )
                        trade.close_trade(
                            exit_date=current_date,
                            exit_price=price,
                            exit_quantity=sell_quantity,
                            commission=commission,
                            slippage=slippage,
                            reason='signal'
                        )
                        new_trades.append(trade)
            
            except Exception as e:
                self.logger.warning(f"Failed to execute trade for {symbol}: {str(e)}")
                continue
        
        return updated_cash, updated_positions, new_trades
    
    def _calculate_position_size(self, available_cash: Decimal, price: Decimal, config: BacktestConfig) -> Decimal:
        """Calculate position size based on available cash and risk limits"""
        max_position_value = available_cash * (config.max_position_size or Decimal("1.0"))
        return min(max_position_value, available_cash * Decimal("0.95"))  # Leave 5% cash buffer
    
    def _calculate_commission(self, trade_value: Decimal, config: BacktestConfig) -> Decimal:
        """Calculate trading commission"""
        if config.cost_model.value == "percentage":
            return trade_value * config.commission_rate
        else:  # fixed
            return config.fixed_commission
    
    def _calculate_slippage(self, trade_value: Decimal, config: BacktestConfig) -> Decimal:
        """Calculate slippage costs"""
        if config.enable_slippage:
            return trade_value * config.bid_ask_spread + trade_value * config.market_impact
        return Decimal("0")
    
    async def _apply_risk_management(self, cash: Decimal, positions: Dict[str, BacktestPosition], 
                                   config: BacktestConfig, current_date: datetime):
        """Apply risk management rules"""
        # Stop loss checks
        if config.stop_loss_pct:
            positions_to_close = []
            for symbol, position in positions.items():
                loss_pct = position.unrealized_pnl_pct
                if loss_pct <= -config.stop_loss_pct:
                    positions_to_close.append(symbol)
            
            # Close positions that hit stop loss
            for symbol in positions_to_close:
                del positions[symbol]
                self.logger.info(f"Closed {symbol} position due to stop loss")
    
    async def _optimize_strategy_parameters(self, strategy: Strategy, start_date: datetime, 
                                          end_date: datetime, universe: List[str]) -> Strategy:
        """Optimize strategy parameters on training data"""
        # Placeholder for parameter optimization
        # In practice, this would use techniques like grid search, genetic algorithms, etc.
        return strategy
    
    async def _calculate_benchmark_comparison(self, results: BacktestResults):
        """Calculate benchmark comparison metrics"""
        config = results.config
        if not config.benchmark_symbol or not results.snapshots:
            return
        
        try:
            # Load benchmark data
            benchmark_data = await self._load_historical_data(
                [config.benchmark_symbol], config.start_date, config.end_date
            )
            
            if config.benchmark_symbol in benchmark_data:
                benchmark_df = benchmark_data[config.benchmark_symbol]
                
                # Calculate benchmark return
                if len(benchmark_df) > 0:
                    initial_price = benchmark_df.iloc[0]['close_price']
                    final_price = benchmark_df.iloc[-1]['close_price']
                    results.benchmark_return = (final_price - initial_price) / initial_price
                    
                    # Calculate alpha, beta, etc. (simplified)
                    portfolio_returns = [float(s.daily_return) for s in results.snapshots[1:]]
                    benchmark_returns = benchmark_df['close_price'].pct_change().dropna().tolist()
                    
                    if len(portfolio_returns) > 0 and len(benchmark_returns) > 0:
                        # Align lengths
                        min_length = min(len(portfolio_returns), len(benchmark_returns))
                        port_returns = portfolio_returns[:min_length]
                        bench_returns = benchmark_returns[:min_length]
                        
                        # Calculate beta
                        portfolio_var = np.var(port_returns)
                        covariance = np.cov(port_returns, bench_returns)[0][1]
                        benchmark_var = np.var(bench_returns)
                        
                        if benchmark_var > 0:
                            results.beta = Decimal(str(covariance / benchmark_var))
                        
                        # Calculate alpha
                        if results.beta is not None:
                            expected_return = results.benchmark_return * results.beta
                            results.alpha = results.total_return - expected_return
                        
                        # Calculate tracking error and information ratio
                        excess_returns = np.array(port_returns) - np.array(bench_returns)
                        results.tracking_error = Decimal(str(np.std(excess_returns)))
                        
                        if results.tracking_error > 0:
                            results.information_ratio = results.alpha / results.tracking_error
        
        except Exception as e:
            self.logger.warning(f"Failed to calculate benchmark comparison: {str(e)}")
    
    async def get_backtest_status(self, backtest_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a running backtest"""
        if backtest_id in self.running_backtests:
            results = self.running_backtests[backtest_id]
            return {
                "backtest_id": backtest_id,
                "status": results.status.value,
                "progress": 0,  # Would calculate based on current progress
                "start_time": results.start_time,
                "message": results.error_message
            }
        return None
    
    async def cancel_backtest(self, backtest_id: str) -> bool:
        """Cancel a running backtest"""
        if backtest_id in self.running_backtests:
            results = self.running_backtests[backtest_id]
            results.status = BacktestStatus.CANCELLED
            del self.running_backtests[backtest_id]
            return True
        return False
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.market_data_handler:
            await self.market_data_handler.cleanup()
        
        self.logger.info("Backtesting engine cleaned up")