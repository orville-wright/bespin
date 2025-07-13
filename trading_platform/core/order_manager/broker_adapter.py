"""
Broker Adapter

Provides abstraction layer for broker integration.
Integrates with existing alpaca_md.py for Alpaca broker operations.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import os
import sys

# Add the parent directory to sys.path to import alpaca_md
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from ...models import Order

logger = logging.getLogger(__name__)


class BrokerAdapter:
    """
    Adapter for broker integration
    
    Currently supports Alpaca broker via alpaca_md.py integration.
    Can be extended to support other brokers.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.broker_type = self.config.get('broker', 'alpaca')
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Broker connection
        self.broker_client = None
        self.order_tracking: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"BrokerAdapter initialized for {self.broker_type} (simulation: {self.is_simulation})")
    
    async def initialize(self) -> None:
        """Initialize broker connection"""
        try:
            if not self.is_simulation:
                await self._initialize_broker_client()
            
            logger.info("BrokerAdapter initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing BrokerAdapter: {e}")
            raise
    
    async def stop(self) -> None:
        """Clean up broker connection"""
        try:
            if self.broker_client:
                # Clean up broker connection if needed
                pass
            
            logger.info("BrokerAdapter stopped")
            
        except Exception as e:
            logger.error(f"Error stopping BrokerAdapter: {e}")
    
    async def _initialize_broker_client(self) -> None:
        """Initialize the actual broker client"""
        try:
            if self.broker_type == 'alpaca':
                await self._initialize_alpaca_client()
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error initializing broker client: {e}")
            raise
    
    async def _initialize_alpaca_client(self) -> None:
        """Initialize Alpaca broker client"""
        try:
            # Import alpaca_md dynamically to avoid issues if not available
            try:
                from alpaca_md import alpaca_md
                self.broker_client = alpaca_md(inst_id=1, args=self.config)
                logger.info("Alpaca broker client initialized")
            except ImportError as e:
                logger.error(f"Could not import alpaca_md: {e}")
                raise
                
        except Exception as e:
            logger.error(f"Error initializing Alpaca client: {e}")
            raise
    
    async def submit_order(self, order: Order) -> Optional[str]:
        """Submit order to broker and return broker order ID"""
        try:
            if self.is_simulation:
                # Generate fake broker order ID for simulation
                broker_order_id = f"SIM_{order.order_id[:8]}"
                logger.info(f"Simulation order submitted: {broker_order_id}")
                return broker_order_id
            
            if self.broker_type == 'alpaca':
                return await self._submit_alpaca_order(order)
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error submitting order to broker: {e}")
            return None
    
    async def _submit_alpaca_order(self, order: Order) -> Optional[str]:
        """Submit order to Alpaca"""
        try:
            # Convert order to Alpaca format
            alpaca_order = order.to_broker_order()
            
            # In a real implementation, we would use Alpaca's trading API
            # For now, we'll simulate the submission
            
            # This is where you would integrate with Alpaca's trading API:
            # response = await self.broker_client.submit_order(alpaca_order)
            # broker_order_id = response.get('id')
            
            # For demonstration, generate a mock broker order ID
            broker_order_id = f"ALPACA_{order.order_id[:8]}"
            
            # Track the order
            self.order_tracking[broker_order_id] = {
                'internal_order_id': order.order_id,
                'symbol': order.symbol,
                'side': order.side,
                'quantity': order.quantity,
                'submitted_at': datetime.utcnow(),
                'status': 'submitted'
            }
            
            logger.info(f"Alpaca order submitted: {broker_order_id}")
            return broker_order_id
            
        except Exception as e:
            logger.error(f"Error submitting Alpaca order: {e}")
            return None
    
    async def cancel_order(self, broker_order_id: str) -> bool:
        """Cancel order with broker"""
        try:
            if self.is_simulation:
                if broker_order_id in self.order_tracking:
                    self.order_tracking[broker_order_id]['status'] = 'canceled'
                    logger.info(f"Simulation order canceled: {broker_order_id}")
                    return True
                return False
            
            if self.broker_type == 'alpaca':
                return await self._cancel_alpaca_order(broker_order_id)
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error canceling order with broker: {e}")
            return False
    
    async def _cancel_alpaca_order(self, broker_order_id: str) -> bool:
        """Cancel Alpaca order"""
        try:
            # In real implementation:
            # response = await self.broker_client.cancel_order(broker_order_id)
            # success = response.get('success', False)
            
            # For demonstration
            if broker_order_id in self.order_tracking:
                self.order_tracking[broker_order_id]['status'] = 'canceled'
                logger.info(f"Alpaca order canceled: {broker_order_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error canceling Alpaca order: {e}")
            return False
    
    async def get_order_updates(self) -> Dict[str, Dict[str, Any]]:
        """Get order status updates from broker"""
        try:
            if self.is_simulation:
                return await self._get_simulation_updates()
            
            if self.broker_type == 'alpaca':
                return await self._get_alpaca_updates()
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error getting order updates: {e}")
            return {}
    
    async def _get_simulation_updates(self) -> Dict[str, Dict[str, Any]]:
        """Get simulated order updates"""
        updates = {}
        
        # Simple simulation logic - randomly fill some orders
        for broker_order_id, order_data in self.order_tracking.items():
            if order_data['status'] == 'submitted':
                # Simulate immediate fill for demonstration
                order_data['status'] = 'filled'
                order_data['filled_quantity'] = order_data['quantity']
                order_data['avg_fill_price'] = 100.0  # Mock price
                order_data['filled_at'] = datetime.utcnow()
                
                updates[broker_order_id] = {
                    'status': 'filled',
                    'filled_quantity': float(order_data['filled_quantity']),
                    'avg_fill_price': order_data['avg_fill_price'],
                    'filled_at': order_data['filled_at']
                }
        
        return updates
    
    async def _get_alpaca_updates(self) -> Dict[str, Dict[str, Any]]:
        """Get order updates from Alpaca"""
        updates = {}
        
        try:
            # In real implementation, query Alpaca for order status updates
            # orders_response = await self.broker_client.get_orders()
            
            # For demonstration, simulate some updates
            for broker_order_id, order_data in self.order_tracking.items():
                if order_data['status'] == 'submitted':
                    # Mock update
                    updates[broker_order_id] = {
                        'status': 'filled',
                        'filled_quantity': float(order_data['quantity']),
                        'avg_fill_price': 100.0,
                        'filled_at': datetime.utcnow()
                    }
                    order_data['status'] = 'filled'
        
        except Exception as e:
            logger.error(f"Error getting Alpaca updates: {e}")
        
        return updates
    
    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information from broker"""
        try:
            if self.is_simulation:
                return {
                    'account_id': 'SIMULATION',
                    'buying_power': 100000.0,
                    'cash': 100000.0,
                    'portfolio_value': 100000.0,
                    'day_trade_count': 0
                }
            
            if self.broker_type == 'alpaca':
                return await self._get_alpaca_account()
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            return {}
    
    async def _get_alpaca_account(self) -> Dict[str, Any]:
        """Get Alpaca account information"""
        try:
            # In real implementation:
            # account = await self.broker_client.get_account()
            
            # Mock account data
            return {
                'account_id': 'ALPACA_DEMO',
                'buying_power': 50000.0,
                'cash': 25000.0,
                'portfolio_value': 75000.0,
                'day_trade_count': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting Alpaca account: {e}")
            return {}
    
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions from broker"""
        try:
            if self.is_simulation:
                return []  # No positions in simulation by default
            
            if self.broker_type == 'alpaca':
                return await self._get_alpaca_positions()
            else:
                raise ValueError(f"Unsupported broker type: {self.broker_type}")
                
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    async def _get_alpaca_positions(self) -> List[Dict[str, Any]]:
        """Get positions from Alpaca"""
        try:
            # In real implementation:
            # positions = await self.broker_client.get_positions()
            
            # Mock positions
            return []
            
        except Exception as e:
            logger.error(f"Error getting Alpaca positions: {e}")
            return []