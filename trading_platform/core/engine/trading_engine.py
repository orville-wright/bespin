"""
Core Trading Engine

Coordinates strategy execution, order management, and real-time data processing.
Integrates with existing aop.py orchestrator and data engines.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import uuid

from ...models import Order, Position, Strategy, OrderCreate, PositionCreate
from ..order_manager.order_manager import OrderManager
from ..position_manager.position_manager import PositionManager
from ..strategy_executor import StrategyExecutor
from ..market_data_handler import MarketDataHandler

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Core trading engine that coordinates all trading operations
    
    Responsibilities:
    - Strategy signal generation and execution
    - Order management and broker integration
    - Position tracking and P&L calculation
    - Real-time data processing
    - Risk management enforcement
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.engine_id = str(uuid.uuid4())
        self.is_running = False
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Core components
        self.order_manager = OrderManager(config=self.config)
        self.position_manager = PositionManager(config=self.config)
        self.strategy_executor = StrategyExecutor(config=self.config)
        self.market_data_handler = MarketDataHandler(config=self.config)
        
        # State tracking
        self.active_strategies: Dict[str, Strategy] = {}
        self.subscribed_symbols: set = set()
        self.performance_metrics = {
            'orders_processed': 0,
            'positions_opened': 0,
            'positions_closed': 0,
            'total_pnl': Decimal('0'),
            'total_fees': Decimal('0'),
            'started_at': None,
            'last_update': None
        }
        
        logger.info(f"Trading engine initialized: {self.engine_id} (simulation: {self.is_simulation})")
    
    async def start(self) -> None:
        """Start the trading engine"""
        if self.is_running:
            logger.warning("Trading engine is already running")
            return
        
        logger.info("Starting trading engine...")
        self.is_running = True
        self.performance_metrics['started_at'] = datetime.utcnow()
        
        try:
            # Initialize components
            await self.order_manager.initialize()
            await self.position_manager.initialize()
            await self.strategy_executor.initialize()
            await self.market_data_handler.initialize()
            
            # Start main processing loop
            await self._run_main_loop()
            
        except Exception as e:
            logger.error(f"Error starting trading engine: {e}")
            await self.stop()
            raise
    
    async def stop(self) -> None:
        """Stop the trading engine gracefully"""
        if not self.is_running:
            return
        
        logger.info("Stopping trading engine...")
        self.is_running = False
        
        try:
            # Stop components in reverse order
            await self.market_data_handler.stop()
            await self.strategy_executor.stop()
            await self.position_manager.stop()
            await self.order_manager.stop()
            
            logger.info("Trading engine stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping trading engine: {e}")
    
    async def _run_main_loop(self) -> None:
        """Main processing loop"""
        logger.info("Starting main trading loop...")
        
        while self.is_running:
            try:
                # Process market data updates
                await self._process_market_data()
                
                # Execute strategy signals
                await self._execute_strategies()
                
                # Process order updates
                await self._process_orders()
                
                # Update positions
                await self._update_positions()
                
                # Update performance metrics
                self._update_metrics()
                
                # Short sleep to prevent busy waiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error in main trading loop: {e}")
                await asyncio.sleep(1)  # Longer sleep on error
    
    async def _process_market_data(self) -> None:
        """Process incoming market data updates"""
        try:
            # Get latest market data for subscribed symbols
            market_updates = await self.market_data_handler.get_latest_updates()
            
            for symbol, data in market_updates.items():
                # Update position values
                await self.position_manager.update_position_prices(symbol, data.get('price'))
                
                # Send data to strategies
                await self.strategy_executor.process_market_data(symbol, data)
                
        except Exception as e:
            logger.error(f"Error processing market data: {e}")
    
    async def _execute_strategies(self) -> None:
        """Execute active strategies and process signals"""
        try:
            # Get strategy signals
            signals = await self.strategy_executor.get_signals()
            
            for signal in signals:
                await self._process_strategy_signal(signal)
                
        except Exception as e:
            logger.error(f"Error executing strategies: {e}")
    
    async def _process_strategy_signal(self, signal: Dict[str, Any]) -> None:
        """Process a strategy signal and generate orders"""
        try:
            strategy_id = signal['strategy_id']
            symbol = signal['symbol']
            action = signal['action']  # 'BUY', 'SELL', 'HOLD'
            quantity = signal.get('quantity', 0)
            price = signal.get('price')
            order_type = signal.get('order_type', 'market')
            
            if action == 'HOLD':
                return
            
            # Create order from signal
            order_data = OrderCreate(
                strategy_id=strategy_id,
                symbol=symbol,
                side=action.lower(),
                order_type=order_type,
                quantity=Decimal(str(quantity)),
                price=Decimal(str(price)) if price else None,
                is_simulation=self.is_simulation
            )
            
            # Submit order
            order = await self.order_manager.create_order(order_data)
            
            if order:
                logger.info(f"Order created from strategy signal: {order.order_id}")
                self.performance_metrics['orders_processed'] += 1
                
        except Exception as e:
            logger.error(f"Error processing strategy signal: {e}")
    
    async def _process_orders(self) -> None:
        """Process order updates from broker"""
        try:
            # Get order updates
            order_updates = await self.order_manager.process_updates()
            
            for order_id, update in order_updates.items():
                if update.get('status') == 'filled':
                    await self._handle_order_fill(order_id, update)
                    
        except Exception as e:
            logger.error(f"Error processing orders: {e}")
    
    async def _handle_order_fill(self, order_id: str, fill_data: Dict[str, Any]) -> None:
        """Handle order fill and update/create positions"""
        try:
            order = await self.order_manager.get_order(order_id)
            if not order:
                logger.error(f"Order not found: {order_id}")
                return
            
            # Update or create position
            position = await self.position_manager.handle_order_fill(order, fill_data)
            
            if position:
                if fill_data.get('position_opened'):
                    self.performance_metrics['positions_opened'] += 1
                elif fill_data.get('position_closed'):
                    self.performance_metrics['positions_closed'] += 1
                    
        except Exception as e:
            logger.error(f"Error handling order fill: {e}")
    
    async def _update_positions(self) -> None:
        """Update position valuations and P&L"""
        try:
            await self.position_manager.update_all_positions()
            
        except Exception as e:
            logger.error(f"Error updating positions: {e}")
    
    def _update_metrics(self) -> None:
        """Update performance metrics"""
        self.performance_metrics['last_update'] = datetime.utcnow()
    
    # Public API methods
    
    async def add_strategy(self, strategy: Strategy) -> bool:
        """Add a strategy to the engine"""
        try:
            self.active_strategies[strategy.strategy_id] = strategy
            await self.strategy_executor.add_strategy(strategy)
            
            # Subscribe to required symbols
            for symbol in strategy.symbols:
                await self.subscribe_symbol(symbol)
            
            logger.info(f"Strategy added: {strategy.strategy_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding strategy: {e}")
            return False
    
    async def remove_strategy(self, strategy_id: str) -> bool:
        """Remove a strategy from the engine"""
        try:
            if strategy_id in self.active_strategies:
                del self.active_strategies[strategy_id]
                await self.strategy_executor.remove_strategy(strategy_id)
                logger.info(f"Strategy removed: {strategy_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error removing strategy: {e}")
            return False
    
    async def subscribe_symbol(self, symbol: str) -> None:
        """Subscribe to market data for a symbol"""
        self.subscribed_symbols.add(symbol)
        await self.market_data_handler.subscribe(symbol)
    
    async def unsubscribe_symbol(self, symbol: str) -> None:
        """Unsubscribe from market data for a symbol"""
        self.subscribed_symbols.discard(symbol)
        await self.market_data_handler.unsubscribe(symbol)
    
    async def get_positions(self, strategy_id: str = None) -> List[Position]:
        """Get current positions"""
        return await self.position_manager.get_positions(strategy_id=strategy_id)
    
    async def get_orders(self, strategy_id: str = None, status: str = None) -> List[Order]:
        """Get orders"""
        return await self.order_manager.get_orders(strategy_id=strategy_id, status=status)
    
    async def create_manual_order(self, order_data: OrderCreate) -> Optional[Order]:
        """Create a manual order (not from strategy)"""
        order_data.strategy_id = order_data.strategy_id or "manual"
        return await self.order_manager.create_order(order_data)
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order"""
        return await self.order_manager.cancel_order(order_id)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        metrics = self.performance_metrics.copy()
        
        # Add current totals from position manager
        if hasattr(self.position_manager, 'get_total_pnl'):
            metrics['total_pnl'] = self.position_manager.get_total_pnl()
        
        return metrics
    
    def get_status(self) -> Dict[str, Any]:
        """Get current engine status"""
        return {
            'engine_id': self.engine_id,
            'is_running': self.is_running,
            'is_simulation': self.is_simulation,
            'active_strategies': len(self.active_strategies),
            'subscribed_symbols': len(self.subscribed_symbols),
            'performance': self.get_performance_metrics()
        }