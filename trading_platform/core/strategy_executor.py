"""
Strategy Executor

Manages strategy lifecycle, signal generation, and execution coordination.
Integrates with the strategy framework and processes market data.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import uuid

from ..models import Strategy, StrategyStatus, StrategySignal

logger = logging.getLogger(__name__)


class StrategyExecutor:
    """
    Executes trading strategies and manages strategy lifecycle
    
    Responsibilities:
    - Strategy registration and lifecycle management
    - Market data distribution to strategies
    - Signal generation and collection
    - Strategy performance tracking
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Strategy management
        self.strategies: Dict[str, Strategy] = {}
        self.strategy_instances: Dict[str, Any] = {}  # Actual strategy objects
        self.strategy_signals: List[Dict[str, Any]] = []
        
        # Performance tracking
        self.stats = {
            'strategies_active': 0,
            'signals_generated': 0,
            'signals_executed': 0,
            'total_strategy_pnl': Decimal('0'),
            'last_signal_time': None
        }
        
        logger.info(f"StrategyExecutor initialized (simulation: {self.is_simulation})")
    
    async def initialize(self) -> None:
        """Initialize the strategy executor"""
        try:
            # Initialize strategy framework
            await self._initialize_strategy_framework()
            logger.info("StrategyExecutor initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing StrategyExecutor: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the strategy executor"""
        try:
            # Stop all strategies
            for strategy_id in list(self.strategies.keys()):
                await self.remove_strategy(strategy_id)
            
            logger.info("StrategyExecutor stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping StrategyExecutor: {e}")
    
    async def _initialize_strategy_framework(self) -> None:
        """Initialize the strategy framework and load built-in strategies"""
        try:
            # Load available strategy classes
            await self._load_strategy_classes()
            
        except Exception as e:
            logger.error(f"Error initializing strategy framework: {e}")
            raise
    
    async def _load_strategy_classes(self) -> None:
        """Load available strategy classes"""
        # In a full implementation, this would dynamically load strategy classes
        # from the strategies directory
        pass
    
    async def add_strategy(self, strategy: Strategy) -> bool:
        """Add and start a strategy"""
        try:
            strategy_id = strategy.strategy_id
            
            # Validate strategy
            if not self._validate_strategy(strategy):
                logger.error(f"Strategy validation failed: {strategy_id}")
                return False
            
            # Store strategy metadata
            self.strategies[strategy_id] = strategy
            
            # Create strategy instance
            strategy_instance = await self._create_strategy_instance(strategy)
            if not strategy_instance:
                logger.error(f"Failed to create strategy instance: {strategy_id}")
                return False
            
            self.strategy_instances[strategy_id] = strategy_instance
            
            # Initialize strategy
            await self._initialize_strategy(strategy_id)
            
            # Update strategy status
            strategy.status = StrategyStatus.ACTIVE
            self.stats['strategies_active'] += 1
            
            logger.info(f"Strategy added and started: {strategy_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding strategy: {e}")
            return False
    
    async def remove_strategy(self, strategy_id: str) -> bool:
        """Remove and stop a strategy"""
        try:
            if strategy_id not in self.strategies:
                logger.warning(f"Strategy not found: {strategy_id}")
                return False
            
            # Stop strategy instance
            if strategy_id in self.strategy_instances:
                await self._stop_strategy(strategy_id)
                del self.strategy_instances[strategy_id]
            
            # Update strategy status
            strategy = self.strategies[strategy_id]
            strategy.status = StrategyStatus.STOPPED
            
            # Remove from active strategies
            del self.strategies[strategy_id]
            self.stats['strategies_active'] -= 1
            
            logger.info(f"Strategy removed: {strategy_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing strategy: {e}")
            return False
    
    async def _create_strategy_instance(self, strategy: Strategy) -> Optional[Any]:
        """Create an instance of the strategy"""
        try:
            # This is a simplified implementation
            # In a real system, this would instantiate the actual strategy class
            
            strategy_instance = {
                'id': strategy.strategy_id,
                'name': strategy.name,
                'type': strategy.strategy_type,
                'symbols': strategy.symbols,
                'parameters': strategy.parameters or {},
                'last_signal': None,
                'market_data': {},
                'state': 'initialized'
            }
            
            return strategy_instance
            
        except Exception as e:
            logger.error(f"Error creating strategy instance: {e}")
            return None
    
    async def _initialize_strategy(self, strategy_id: str) -> None:
        """Initialize a strategy instance"""
        try:
            strategy_instance = self.strategy_instances.get(strategy_id)
            if strategy_instance:
                strategy_instance['state'] = 'active'
                logger.info(f"Strategy initialized: {strategy_id}")
                
        except Exception as e:
            logger.error(f"Error initializing strategy {strategy_id}: {e}")
    
    async def _stop_strategy(self, strategy_id: str) -> None:
        """Stop a strategy instance"""
        try:
            strategy_instance = self.strategy_instances.get(strategy_id)
            if strategy_instance:
                strategy_instance['state'] = 'stopped'
                logger.info(f"Strategy stopped: {strategy_id}")
                
        except Exception as e:
            logger.error(f"Error stopping strategy {strategy_id}: {e}")
    
    def _validate_strategy(self, strategy: Strategy) -> bool:
        """Validate strategy configuration"""
        try:
            # Basic validation
            if not strategy.strategy_id:
                logger.error("Strategy must have an ID")
                return False
            
            if not strategy.name:
                logger.error("Strategy must have a name")
                return False
            
            if not strategy.symbols:
                logger.error("Strategy must have symbols")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating strategy: {e}")
            return False
    
    async def process_market_data(self, symbol: str, market_data: Dict[str, Any]) -> None:
        """Process market data and distribute to strategies"""
        try:
            # Distribute market data to interested strategies
            for strategy_id, strategy_instance in self.strategy_instances.items():
                if symbol in strategy_instance['symbols']:
                    await self._send_market_data_to_strategy(strategy_id, symbol, market_data)
                    
        except Exception as e:
            logger.error(f"Error processing market data: {e}")
    
    async def _send_market_data_to_strategy(self, strategy_id: str, symbol: str, market_data: Dict[str, Any]) -> None:
        """Send market data to a specific strategy"""
        try:
            strategy_instance = self.strategy_instances.get(strategy_id)
            if not strategy_instance:
                return
            
            # Update strategy's market data
            strategy_instance['market_data'][symbol] = market_data
            
            # Generate signal if needed
            await self._generate_strategy_signal(strategy_id, symbol, market_data)
            
        except Exception as e:
            logger.error(f"Error sending market data to strategy {strategy_id}: {e}")
    
    async def _generate_strategy_signal(self, strategy_id: str, symbol: str, market_data: Dict[str, Any]) -> None:
        """Generate trading signal from strategy"""
        try:
            strategy_instance = self.strategy_instances.get(strategy_id)
            if not strategy_instance:
                return
            
            # This is a simplified signal generation
            # In a real implementation, this would call the strategy's signal generation method
            
            signal = await self._execute_strategy_logic(strategy_instance, symbol, market_data)
            
            if signal:
                # Store signal
                signal_data = {
                    'strategy_id': strategy_id,
                    'symbol': symbol,
                    'action': signal['action'],  # 'BUY', 'SELL', 'HOLD'
                    'quantity': signal.get('quantity', 0),
                    'price': signal.get('price'),
                    'order_type': signal.get('order_type', 'market'),
                    'confidence': signal.get('confidence', 0.0),
                    'timestamp': datetime.utcnow(),
                    'metadata': signal.get('metadata', {})
                }
                
                self.strategy_signals.append(signal_data)
                self.stats['signals_generated'] += 1
                self.stats['last_signal_time'] = datetime.utcnow()
                
                logger.info(f"Signal generated: {strategy_id} {symbol} {signal['action']}")
                
        except Exception as e:
            logger.error(f"Error generating strategy signal: {e}")
    
    async def _execute_strategy_logic(self, strategy_instance: Dict[str, Any], symbol: str, market_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Execute strategy logic and return signal"""
        try:
            # This is a simplified example strategy logic
            # In a real implementation, this would call the actual strategy class methods
            
            strategy_type = strategy_instance.get('type', 'simple')
            
            if strategy_type == 'simple_momentum':
                return await self._simple_momentum_strategy(strategy_instance, symbol, market_data)
            elif strategy_type == 'mean_reversion':
                return await self._mean_reversion_strategy(strategy_instance, symbol, market_data)
            else:
                return await self._default_strategy(strategy_instance, symbol, market_data)
                
        except Exception as e:
            logger.error(f"Error executing strategy logic: {e}")
            return None
    
    async def _simple_momentum_strategy(self, strategy_instance: Dict[str, Any], symbol: str, market_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Simple momentum strategy example"""
        try:
            current_price = market_data.get('price', 0)
            
            # Simple logic: buy if price is above a threshold
            if current_price > 100:
                return {
                    'action': 'BUY',
                    'quantity': 100,
                    'order_type': 'market',
                    'confidence': 0.7
                }
            elif current_price < 95:
                return {
                    'action': 'SELL',
                    'quantity': 100,
                    'order_type': 'market',
                    'confidence': 0.6
                }
            
            return {'action': 'HOLD'}
            
        except Exception as e:
            logger.error(f"Error in momentum strategy: {e}")
            return None
    
    async def _mean_reversion_strategy(self, strategy_instance: Dict[str, Any], symbol: str, market_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Simple mean reversion strategy example"""
        try:
            current_price = market_data.get('price', 0)
            
            # Simple logic: sell if price is too high, buy if too low
            if current_price > 110:
                return {
                    'action': 'SELL',
                    'quantity': 50,
                    'order_type': 'limit',
                    'price': current_price,
                    'confidence': 0.8
                }
            elif current_price < 90:
                return {
                    'action': 'BUY',
                    'quantity': 50,
                    'order_type': 'limit',
                    'price': current_price,
                    'confidence': 0.8
                }
            
            return {'action': 'HOLD'}
            
        except Exception as e:
            logger.error(f"Error in mean reversion strategy: {e}")
            return None
    
    async def _default_strategy(self, strategy_instance: Dict[str, Any], symbol: str, market_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Default strategy - mostly holds"""
        return {'action': 'HOLD'}
    
    async def get_signals(self) -> List[Dict[str, Any]]:
        """Get and clear pending strategy signals"""
        try:
            signals = self.strategy_signals.copy()
            self.strategy_signals.clear()
            return signals
            
        except Exception as e:
            logger.error(f"Error getting signals: {e}")
            return []
    
    # Public API methods
    
    def get_active_strategies(self) -> List[Strategy]:
        """Get list of active strategies"""
        return [s for s in self.strategies.values() if s.status == StrategyStatus.ACTIVE]
    
    def get_strategy(self, strategy_id: str) -> Optional[Strategy]:
        """Get strategy by ID"""
        return self.strategies.get(strategy_id)
    
    def get_strategy_performance(self, strategy_id: str) -> Dict[str, Any]:
        """Get performance metrics for a strategy"""
        # This would calculate strategy-specific performance metrics
        return {
            'strategy_id': strategy_id,
            'signals_generated': 0,  # Would track per strategy
            'success_rate': 0.0,
            'total_pnl': 0.0,
            'sharpe_ratio': 0.0
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get strategy executor statistics"""
        return self.stats.copy()