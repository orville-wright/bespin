"""
Order Manager

Manages order lifecycle, broker integration, and order state tracking.
Integrates with Alpaca broker via alpaca_md.py.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import uuid

from ...models import Order, OrderCreate, OrderUpdate, OrderStatus
from .broker_adapter import BrokerAdapter

logger = logging.getLogger(__name__)


class OrderManager:
    """
    Manages all order operations and broker interactions
    
    Responsibilities:
    - Order creation and validation
    - Broker communication
    - Order status tracking
    - Fill processing
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Order storage (in production, this would be database-backed)
        self.orders: Dict[str, Order] = {}
        self.pending_orders: Dict[str, Order] = {}
        
        # Broker integration
        self.broker_adapter = BrokerAdapter(config=self.config)
        
        # Performance tracking
        self.stats = {
            'orders_created': 0,
            'orders_filled': 0,
            'orders_canceled': 0,
            'orders_rejected': 0,
            'total_volume': Decimal('0'),
            'total_fees': Decimal('0')
        }
        
        logger.info(f"OrderManager initialized (simulation: {self.is_simulation})")
    
    async def initialize(self) -> None:
        """Initialize the order manager"""
        try:
            await self.broker_adapter.initialize()
            logger.info("OrderManager initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing OrderManager: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the order manager"""
        try:
            # Cancel all pending orders
            for order_id in list(self.pending_orders.keys()):
                await self.cancel_order(order_id)
            
            await self.broker_adapter.stop()
            logger.info("OrderManager stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping OrderManager: {e}")
    
    async def create_order(self, order_data: OrderCreate) -> Optional[Order]:
        """Create and submit a new order"""
        try:
            # Generate order ID
            order_id = str(uuid.uuid4())
            
            # Create order object
            order = Order(
                order_id=order_id,
                strategy_id=order_data.strategy_id,
                symbol=order_data.symbol,
                side=order_data.side,
                order_type=order_data.order_type,
                quantity=order_data.quantity,
                price=order_data.price,
                stop_price=order_data.stop_price,
                time_in_force=order_data.time_in_force,
                metadata=order_data.metadata or {},
                is_simulation=order_data.is_simulation or self.is_simulation
            )
            
            # Validate order
            if not self._validate_order(order):
                logger.error(f"Order validation failed: {order_id}")
                return None
            
            # Store order
            self.orders[order_id] = order
            self.pending_orders[order_id] = order
            
            # Submit to broker
            if not order.is_simulation:
                broker_order_id = await self.broker_adapter.submit_order(order)
                if broker_order_id:
                    order.broker_order_id = broker_order_id
                    order.status = OrderStatus.SUBMITTED
                    order.submitted_at = datetime.utcnow()
                else:
                    order.status = OrderStatus.REJECTED
                    del self.pending_orders[order_id]
            else:
                # Simulation mode - immediately mark as submitted
                order.status = OrderStatus.SUBMITTED
                order.submitted_at = datetime.utcnow()
                
                # In simulation, we can simulate fills
                await self._simulate_order_fill(order)
            
            self.stats['orders_created'] += 1
            logger.info(f"Order created: {order_id} ({order.symbol} {order.side} {order.quantity})")
            
            return order
            
        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return None
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        try:
            if order_id not in self.orders:
                logger.warning(f"Order not found for cancellation: {order_id}")
                return False
            
            order = self.orders[order_id]
            
            if not order.is_active:
                logger.warning(f"Cannot cancel non-active order: {order_id} (status: {order.status})")
                return False
            
            # Cancel with broker
            if not order.is_simulation and order.broker_order_id:
                success = await self.broker_adapter.cancel_order(order.broker_order_id)
                if not success:
                    logger.error(f"Failed to cancel order with broker: {order_id}")
                    return False
            
            # Update order status
            order.status = OrderStatus.CANCELED
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            
            self.stats['orders_canceled'] += 1
            logger.info(f"Order canceled: {order_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error canceling order: {e}")
            return False
    
    async def process_updates(self) -> Dict[str, Dict[str, Any]]:
        """Process order updates from broker"""
        updates = {}
        
        try:
            if self.is_simulation:
                # In simulation mode, process simulated fills
                updates = await self._process_simulation_updates()
            else:
                # Get updates from broker
                broker_updates = await self.broker_adapter.get_order_updates()
                
                for broker_order_id, update_data in broker_updates.items():
                    order = self._find_order_by_broker_id(broker_order_id)
                    if order:
                        update = await self._process_order_update(order, update_data)
                        if update:
                            updates[order.order_id] = update
            
            return updates
            
        except Exception as e:
            logger.error(f"Error processing order updates: {e}")
            return {}
    
    async def _process_order_update(self, order: Order, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single order update"""
        try:
            # Update order fields
            if 'status' in update_data:
                old_status = order.status
                order.status = OrderStatus(update_data['status'])
                
                if order.status != old_status:
                    logger.info(f"Order {order.order_id} status: {old_status} -> {order.status}")
            
            if 'filled_quantity' in update_data:
                order.filled_quantity = Decimal(str(update_data['filled_quantity']))
            
            if 'avg_fill_price' in update_data:
                order.avg_fill_price = Decimal(str(update_data['avg_fill_price']))
            
            if 'filled_at' in update_data:
                order.filled_at = update_data['filled_at']
            
            # Remove from pending if no longer active
            if not order.is_active and order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]
            
            # Update stats
            if order.status == OrderStatus.FILLED:
                self.stats['orders_filled'] += 1
                self.stats['total_volume'] += order.filled_quantity
            elif order.status == OrderStatus.REJECTED:
                self.stats['orders_rejected'] += 1
            
            return {
                'status': order.status.value,
                'filled_quantity': float(order.filled_quantity),
                'avg_fill_price': float(order.avg_fill_price) if order.avg_fill_price else None,
                'position_opened': update_data.get('position_opened', False),
                'position_closed': update_data.get('position_closed', False)
            }
            
        except Exception as e:
            logger.error(f"Error processing order update: {e}")
            return None
    
    async def _simulate_order_fill(self, order: Order) -> None:
        """Simulate order fill for testing/simulation mode"""
        try:
            # Simple simulation - immediate fill at current market price
            # In reality, this would be more sophisticated
            
            # Simulate small delay
            await asyncio.sleep(0.1)
            
            # Mark as filled
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.filled_at = datetime.utcnow()
            
            # Use order price or simulate market price
            if order.price:
                order.avg_fill_price = order.price
            else:
                # Simulate market price (in real implementation, get from market data)
                order.avg_fill_price = Decimal('100.00')  # Placeholder
            
            logger.info(f"Simulated order fill: {order.order_id}")
            
        except Exception as e:
            logger.error(f"Error simulating order fill: {e}")
    
    async def _process_simulation_updates(self) -> Dict[str, Dict[str, Any]]:
        """Process updates for simulation mode"""
        updates = {}
        
        # Check for filled orders
        for order_id, order in list(self.pending_orders.items()):
            if order.status == OrderStatus.FILLED:
                updates[order_id] = {
                    'status': 'filled',
                    'filled_quantity': float(order.filled_quantity),
                    'avg_fill_price': float(order.avg_fill_price),
                    'position_opened': True  # Simplified logic
                }
                del self.pending_orders[order_id]
        
        return updates
    
    def _validate_order(self, order: Order) -> bool:
        """Validate order before submission"""
        try:
            # Basic validation
            if order.quantity <= 0:
                logger.error(f"Invalid quantity: {order.quantity}")
                return False
            
            if order.order_type == "limit" and not order.price:
                logger.error("Limit order must have price")
                return False
            
            if order.order_type in ["stop", "stop_limit"] and not order.stop_price:
                logger.error("Stop order must have stop price")
                return False
            
            # Symbol validation (basic)
            if not order.symbol or len(order.symbol) < 1:
                logger.error(f"Invalid symbol: {order.symbol}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating order: {e}")
            return False
    
    def _find_order_by_broker_id(self, broker_order_id: str) -> Optional[Order]:
        """Find order by broker order ID"""
        for order in self.orders.values():
            if order.broker_order_id == broker_order_id:
                return order
        return None
    
    # Public API methods
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        return self.orders.get(order_id)
    
    async def get_orders(self, strategy_id: str = None, status: str = None) -> List[Order]:
        """Get orders with optional filtering"""
        orders = list(self.orders.values())
        
        if strategy_id:
            orders = [o for o in orders if o.strategy_id == strategy_id]
        
        if status:
            orders = [o for o in orders if o.status == status]
        
        return orders
    
    async def get_pending_orders(self) -> List[Order]:
        """Get all pending orders"""
        return list(self.pending_orders.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get order manager statistics"""
        return self.stats.copy()