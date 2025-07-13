"""
Advanced Performance Engine
Comprehensive performance analytics and risk calculations
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any, Tuple
import asyncpg
import pandas as pd
import numpy as np
from scipy import stats
import json

from ..models.portfolio_models import (
    PerformanceMetrics, RiskMetrics, AttributionAnalysis,
    PortfolioCalculator
)


class PerformanceEngine:
    """
    Advanced performance analytics engine
    Provides comprehensive portfolio performance analysis, risk metrics, and attribution
    """
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.logger = logging.getLogger(__name__)
        self.calculator = PortfolioCalculator()
        
    async def create_connection(self) -> asyncpg.Connection:
        """Create database connection"""
        return await asyncpg.connect(self.db_connection_string)
    
    async def calculate_comprehensive_performance(
        self, 
        portfolio_id: int, 
        start_date: datetime, 
        end_date: datetime,
        benchmark_symbol: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate comprehensive performance analysis"""
        conn = await self.create_connection()
        try:
            # Get portfolio value history
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            if not portfolio_data:
                return {"error": "No portfolio data found for the specified period"}
            
            # Calculate returns
            values = [row['total_value'] for row in portfolio_data]
            dates = [row['timestamp'] for row in portfolio_data]
            returns = self.calculator.calculate_returns(values)
            
            # Basic performance metrics
            total_return = (values[-1] - values[0]) / values[0] if values[0] != 0 else Decimal('0')
            annualized_return = self._annualize_return(total_return, len(returns))
            
            # Risk metrics
            volatility = self._calculate_volatility(returns)
            sharpe_ratio = self.calculator.calculate_sharpe_ratio(returns)
            sortino_ratio = self._calculate_sortino_ratio(returns)
            max_drawdown = self.calculator.calculate_max_drawdown([v / values[0] for v in values])
            
            # VaR metrics
            var_95 = self.calculator.calculate_var(returns, 0.95)
            var_99 = self.calculator.calculate_var(returns, 0.99)
            cvar_95 = self._calculate_cvar(returns, 0.95)
            
            # Performance analysis
            analysis = {
                "period": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "days": (end_date - start_date).days
                },
                "returns": {
                    "total_return": total_return,
                    "annualized_return": annualized_return,
                    "daily_returns": returns[-252:] if len(returns) > 252 else returns,  # Last year
                    "cumulative_returns": self._calculate_cumulative_returns(returns)
                },
                "risk": {
                    "volatility": volatility,
                    "annualized_volatility": volatility * Decimal(str(np.sqrt(252))),
                    "sharpe_ratio": sharpe_ratio,
                    "sortino_ratio": sortino_ratio,
                    "max_drawdown": max_drawdown,
                    "var_95": var_95,
                    "var_99": var_99,
                    "cvar_95": cvar_95
                },
                "portfolio_stats": {
                    "start_value": values[0],
                    "end_value": values[-1],
                    "min_value": min(values),
                    "max_value": max(values),
                    "avg_value": Decimal(str(np.mean([float(v) for v in values])))
                }
            }
            
            # Add benchmark comparison if provided
            if benchmark_symbol:
                benchmark_analysis = await self._calculate_benchmark_comparison(
                    conn, portfolio_id, benchmark_symbol, start_date, end_date
                )
                analysis["benchmark_comparison"] = benchmark_analysis
            
            return analysis
            
        finally:
            await conn.close()
    
    async def calculate_returns_analysis(
        self, 
        portfolio_id: int, 
        start_date: datetime, 
        end_date: datetime,
        frequency: str = "daily"
    ) -> Dict[str, Any]:
        """Calculate detailed returns analysis"""
        conn = await self.create_connection()
        try:
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            if not portfolio_data:
                return {"error": "No data found"}
            
            df = pd.DataFrame(portfolio_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Resample based on frequency
            if frequency == "weekly":
                df_resampled = df.resample('W').last()
            elif frequency == "monthly":
                df_resampled = df.resample('M').last()
            else:  # daily
                df_resampled = df
            
            # Calculate returns
            df_resampled['returns'] = df_resampled['total_value'].pct_change().dropna()
            
            returns = df_resampled['returns'].tolist()
            returns_stats = {
                "mean": float(np.mean(returns)),
                "median": float(np.median(returns)),
                "std": float(np.std(returns)),
                "skewness": float(stats.skew(returns)),
                "kurtosis": float(stats.kurtosis(returns)),
                "min": float(np.min(returns)),
                "max": float(np.max(returns)),
                "positive_periods": int(np.sum(np.array(returns) > 0)),
                "negative_periods": int(np.sum(np.array(returns) < 0)),
                "win_rate": float(np.sum(np.array(returns) > 0) / len(returns))
            }
            
            # Percentile analysis
            percentiles = [5, 10, 25, 50, 75, 90, 95]
            percentile_values = {
                f"p{p}": float(np.percentile(returns, p)) for p in percentiles
            }
            
            return {
                "frequency": frequency,
                "period_count": len(returns),
                "returns_statistics": returns_stats,
                "percentiles": percentile_values,
                "returns_data": returns
            }
            
        finally:
            await conn.close()
    
    async def calculate_advanced_risk_metrics(
        self, 
        portfolio_id: int, 
        confidence_levels: List[float],
        lookback_days: int = 252
    ) -> Dict[str, Any]:
        """Calculate advanced risk metrics"""
        conn = await self.create_connection()
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=lookback_days)
            
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            if not portfolio_data:
                return {"error": "Insufficient data for risk calculation"}
            
            values = [row['total_value'] for row in portfolio_data]
            returns = self.calculator.calculate_returns(values)
            
            if len(returns) < 30:  # Minimum data requirement
                return {"error": "Insufficient data points for reliable risk metrics"}
            
            risk_metrics = {}
            
            # VaR and CVaR for each confidence level
            for conf in confidence_levels:
                var = self.calculator.calculate_var(returns, conf)
                cvar = self._calculate_cvar(returns, conf)
                risk_metrics[f"var_{int(conf*100)}"] = var
                risk_metrics[f"cvar_{int(conf*100)}"] = cvar
            
            # Advanced risk metrics
            risk_metrics.update({
                "volatility": self._calculate_volatility(returns),
                "downside_deviation": self._calculate_downside_deviation(returns),
                "max_drawdown": self.calculator.calculate_max_drawdown([v / values[0] for v in values]),
                "calmar_ratio": self._calculate_calmar_ratio(returns, values),
                "sortino_ratio": self._calculate_sortino_ratio(returns),
                "treynor_ratio": self._calculate_treynor_ratio(returns),
                "information_ratio": self._calculate_information_ratio(returns),
                "tracking_error": self._calculate_tracking_error(returns),
                "tail_ratio": self._calculate_tail_ratio(returns),
                "pain_ratio": self._calculate_pain_ratio(returns, values)
            })
            
            # Risk attribution by position
            risk_attribution = await self._calculate_risk_attribution(conn, portfolio_id)
            risk_metrics["position_risk_attribution"] = risk_attribution
            
            return risk_metrics
            
        finally:
            await conn.close()
    
    async def calculate_drawdown_analysis(
        self, 
        portfolio_id: int, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Calculate detailed drawdown analysis"""
        conn = await self.create_connection()
        try:
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            if not portfolio_data:
                return {"error": "No data found"}
            
            values = [row['total_value'] for row in portfolio_data]
            dates = [row['timestamp'] for row in portfolio_data]
            
            # Calculate running maximum and drawdowns
            running_max = []
            drawdowns = []
            current_max = values[0]
            
            for value in values:
                if value > current_max:
                    current_max = value
                running_max.append(current_max)
                
                dd = (current_max - value) / current_max if current_max != 0 else Decimal('0')
                drawdowns.append(dd)
            
            # Find drawdown periods
            drawdown_periods = self._identify_drawdown_periods(drawdowns, dates)
            
            # Calculate statistics
            max_drawdown = max(drawdowns) if drawdowns else Decimal('0')
            avg_drawdown = Decimal(str(np.mean([float(dd) for dd in drawdowns if dd > 0]))) if any(dd > 0 for dd in drawdowns) else Decimal('0')
            
            return {
                "max_drawdown": max_drawdown,
                "average_drawdown": avg_drawdown,
                "current_drawdown": drawdowns[-1] if drawdowns else Decimal('0'),
                "drawdown_periods": drawdown_periods,
                "drawdown_series": drawdowns,
                "recovery_time": self._calculate_average_recovery_time(drawdown_periods)
            }
            
        finally:
            await conn.close()
    
    async def calculate_performance_attribution(
        self, 
        portfolio_id: int, 
        start_date: datetime, 
        end_date: datetime,
        attribution_type: str = "brinson",
        benchmark_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate performance attribution analysis"""
        conn = await self.create_connection()
        try:
            if attribution_type == "brinson":
                return await self._calculate_brinson_attribution(
                    conn, portfolio_id, start_date, end_date, benchmark_id
                )
            elif attribution_type == "returns_based":
                return await self._calculate_returns_based_attribution(
                    conn, portfolio_id, start_date, end_date
                )
            else:
                raise ValueError(f"Unknown attribution type: {attribution_type}")
                
        finally:
            await conn.close()
    
    async def compare_with_benchmarks(
        self, 
        portfolio_id: int, 
        benchmark_symbols: List[str],
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Compare portfolio performance with multiple benchmarks"""
        conn = await self.create_connection()
        try:
            # Get portfolio data
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            portfolio_values = [row['total_value'] for row in portfolio_data]
            portfolio_returns = self.calculator.calculate_returns(portfolio_values)
            
            comparisons = {}
            
            for symbol in benchmark_symbols:
                benchmark_data = await self._get_benchmark_data(conn, symbol, start_date, end_date)
                if benchmark_data:
                    benchmark_values = [row['price'] for row in benchmark_data]
                    benchmark_returns = self.calculator.calculate_returns(benchmark_values)
                    
                    # Calculate comparison metrics
                    comparison = self._calculate_benchmark_metrics(
                        portfolio_returns, benchmark_returns, symbol
                    )
                    comparisons[symbol] = comparison
            
            return {
                "portfolio_id": portfolio_id,
                "period": {"start": start_date, "end": end_date},
                "benchmark_comparisons": comparisons
            }
            
        finally:
            await conn.close()
    
    async def calculate_rolling_metrics(
        self, 
        portfolio_id: int, 
        window_days: int,
        metrics: List[str], 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Calculate rolling performance metrics"""
        conn = await self.create_connection()
        try:
            portfolio_data = await self._get_portfolio_time_series(
                conn, portfolio_id, start_date, end_date
            )
            
            if len(portfolio_data) < window_days:
                return {"error": "Insufficient data for rolling analysis"}
            
            df = pd.DataFrame(portfolio_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df['returns'] = df['total_value'].pct_change()
            
            rolling_data = {}
            
            for metric in metrics:
                if metric == "returns":
                    rolling_data[metric] = df['returns'].rolling(window=window_days).mean().tolist()
                elif metric == "volatility":
                    rolling_data[metric] = df['returns'].rolling(window=window_days).std().tolist()
                elif metric == "sharpe":
                    rolling_returns = df['returns'].rolling(window=window_days).mean()
                    rolling_vol = df['returns'].rolling(window=window_days).std()
                    rolling_data[metric] = (rolling_returns / rolling_vol * np.sqrt(252)).tolist()
                elif metric == "max_drawdown":
                    rolling_data[metric] = self._calculate_rolling_max_drawdown(
                        df['total_value'], window_days
                    )
            
            return {
                "window_days": window_days,
                "metrics": rolling_data,
                "dates": df.index.tolist()
            }
            
        finally:
            await conn.close()
    
    async def run_stress_test(
        self, 
        portfolio_id: int, 
        scenario: str, 
        shock_magnitude: float
    ) -> Dict[str, Any]:
        """Run stress test scenarios"""
        conn = await self.create_connection()
        try:
            # Get current positions
            positions = await conn.fetch(
                """
                SELECT p.*, i.symbol, i.sector, i.instrument_type 
                FROM current_positions p
                JOIN instruments i ON p.instrument_id = i.id
                WHERE p.portfolio_id = $1 AND p.quantity != 0
                """,
                portfolio_id
            )
            
            if not positions:
                return {"error": "No positions found"}
            
            stress_results = {}
            
            if scenario == "market_crash":
                # Apply uniform shock to all equity positions
                for pos in positions:
                    if pos['instrument_type'] == 'stock':
                        shocked_value = pos['market_value'] * (1 + shock_magnitude)
                        stress_results[pos['symbol']] = {
                            "original_value": pos['market_value'],
                            "stressed_value": shocked_value,
                            "pnl_impact": shocked_value - pos['market_value']
                        }
            
            elif scenario == "sector_rotation":
                # Apply different shocks by sector
                sector_shocks = {
                    "Technology": shock_magnitude * 1.5,
                    "Healthcare": shock_magnitude * 0.5,
                    "Financials": shock_magnitude * 1.2,
                    "Energy": shock_magnitude * 2.0
                }
                
                for pos in positions:
                    sector = pos['sector'] or 'Unknown'
                    sector_shock = sector_shocks.get(sector, shock_magnitude)
                    shocked_value = pos['market_value'] * (1 + sector_shock)
                    stress_results[pos['symbol']] = {
                        "sector": sector,
                        "sector_shock": sector_shock,
                        "original_value": pos['market_value'],
                        "stressed_value": shocked_value,
                        "pnl_impact": shocked_value - pos['market_value']
                    }
            
            # Calculate total portfolio impact
            total_original = sum(pos['market_value'] for pos in positions)
            total_stressed = sum(result['stressed_value'] for result in stress_results.values())
            total_impact = total_stressed - total_original
            
            return {
                "scenario": scenario,
                "shock_magnitude": shock_magnitude,
                "position_impacts": stress_results,
                "portfolio_impact": {
                    "original_value": total_original,
                    "stressed_value": total_stressed,
                    "total_pnl_impact": total_impact,
                    "percentage_impact": (total_impact / total_original * 100) if total_original != 0 else 0
                }
            }
            
        finally:
            await conn.close()
    
    async def generate_performance_report(
        self, 
        portfolio_id: int, 
        report_type: str, 
        period: str, 
        report_id: str
    ):
        """Generate comprehensive performance report (background task)"""
        try:
            # This would typically save to file/database
            # For now, just log the completion
            self.logger.info(f"Generated {report_type} performance report {report_id} for portfolio {portfolio_id}")
        except Exception as e:
            self.logger.error(f"Error generating performance report: {str(e)}")
    
    # Helper methods
    async def _get_portfolio_time_series(
        self, 
        conn: asyncpg.Connection, 
        portfolio_id: int, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get portfolio time series data"""
        rows = await conn.fetch(
            """
            SELECT timestamp, total_value, total_return, daily_return
            FROM portfolio_performance
            WHERE portfolio_id = $1 
            AND timestamp BETWEEN $2 AND $3
            ORDER BY timestamp ASC
            """,
            portfolio_id, start_date, end_date
        )
        return [dict(row) for row in rows]
    
    def _calculate_volatility(self, returns: List[Decimal]) -> Decimal:
        """Calculate volatility"""
        if not returns:
            return Decimal('0')
        return Decimal(str(np.std([float(r) for r in returns])))
    
    def _calculate_cvar(self, returns: List[Decimal], confidence: float) -> Decimal:
        """Calculate Conditional Value at Risk"""
        if not returns:
            return Decimal('0')
        
        returns_array = np.array([float(r) for r in returns])
        var_threshold = np.percentile(returns_array, (1 - confidence) * 100)
        cvar = np.mean(returns_array[returns_array <= var_threshold])
        return Decimal(str(cvar))
    
    def _calculate_sortino_ratio(self, returns: List[Decimal], risk_free_rate: Decimal = Decimal('0.02')) -> Decimal:
        """Calculate Sortino ratio"""
        if not returns:
            return Decimal('0')
        
        returns_array = np.array([float(r) for r in returns])
        excess_returns = returns_array - float(risk_free_rate) / 252
        downside_returns = excess_returns[excess_returns < 0]
        
        if len(downside_returns) == 0:
            return Decimal('0')
        
        downside_deviation = np.std(downside_returns)
        if downside_deviation == 0:
            return Decimal('0')
        
        return Decimal(str(np.mean(excess_returns) / downside_deviation * np.sqrt(252)))
    
    def _annualize_return(self, total_return: Decimal, periods: int) -> Decimal:
        """Annualize return based on number of periods"""
        if periods == 0:
            return Decimal('0')
        
        years = periods / 252  # Assuming 252 trading days per year
        if years <= 0:
            return total_return
        
        return (1 + total_return) ** (1 / years) - 1
    
    def _calculate_cumulative_returns(self, returns: List[Decimal]) -> List[Decimal]:
        """Calculate cumulative returns"""
        if not returns:
            return []
        
        cumulative = [Decimal('0')]
        for ret in returns:
            cumulative.append((1 + cumulative[-1]) * (1 + ret) - 1)
        
        return cumulative[1:]  # Remove initial 0
    
    # Additional helper methods would be implemented here...