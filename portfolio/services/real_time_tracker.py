"""
Real-time Position Tracker
Handles real-time position updates and market data integration
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any, Optional, Callable
import json
import websockets
from dataclasses import asdict

from ..models.portfolio_models import Position, MarketData, PerformanceMetrics
from .portfolio_service import PortfolioService


class RealTimeTracker:
    """
    Real-time position tracking and market data processing
    Integrates with market data feeds and updates positions in real-time
    """
    
    def __init__(self, portfolio_service: PortfolioService):
        self.portfolio_service = portfolio_service
        self.logger = logging.getLogger(__name__)
        self.active_subscriptions: Dict[str, bool] = {}
        self.position_cache: Dict[int, Dict[int, Position]] = {}  # portfolio_id -> instrument_id -> Position
        self.callbacks: List[Callable] = []
        
    async def start_tracking(self, portfolio_ids: List[int]):
        """Start real-time tracking for specified portfolios"""
        try:
            # Initialize position cache
            for portfolio_id in portfolio_ids:
                await self._initialize_portfolio_cache(portfolio_id)
            
            # Start background tasks
            tasks = [
                asyncio.create_task(self._market_data_processor()),
                asyncio.create_task(self._position_updater()),
                asyncio.create_task(self._performance_calculator())
            ]
            
            self.logger.info(f"Started real-time tracking for portfolios: {portfolio_ids}")
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"Error starting real-time tracking: {str(e)}")
            raise
    
    async def _initialize_portfolio_cache(self, portfolio_id: int):
        """Initialize position cache for a portfolio"""
        try:
            positions = await self.portfolio_service.get_current_positions(portfolio_id)
            self.position_cache[portfolio_id] = {}
            
            for position in positions:
                self.position_cache[portfolio_id][position.instrument_id] = position
            
            self.logger.info(f"Initialized cache for portfolio {portfolio_id} with {len(positions)} positions")
            
        except Exception as e:
            self.logger.error(f"Error initializing cache for portfolio {portfolio_id}: {str(e)}")
    
    async def _market_data_processor(self):
        """Process incoming market data and update positions"""
        while True:
            try:
                # Simulate market data reception (replace with actual market data feed)
                market_data = await self._fetch_market_data()
                
                if market_data:
                    await self._process_market_data_batch(market_data)
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                self.logger.error(f"Error in market data processor: {str(e)}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _fetch_market_data(self) -> List[MarketData]:
        """Fetch latest market data (placeholder - integrate with actual data source)"""
        # This would integrate with your market data sources (Alpaca, Yahoo Finance, etc.)
        # For now, return empty list
        return []
    
    async def _process_market_data_batch(self, market_data: List[MarketData]):
        """Process a batch of market data updates"""
        try:
            # Update database with new market data
            await self.portfolio_service.update_market_data(market_data)
            
            # Update position cache with new prices
            for data in market_data:
                await self._update_positions_for_instrument(data.instrument_id, data.close_price)
            
            # Notify callbacks
            await self._notify_callbacks("market_data_update", {
                "timestamp": datetime.now(timezone.utc),
                "instruments_updated": [data.instrument_id for data in market_data]
            })
            
        except Exception as e:
            self.logger.error(f"Error processing market data batch: {str(e)}")
    
    async def _update_positions_for_instrument(self, instrument_id: int, new_price: Decimal):
        """Update all positions for a specific instrument with new price"""
        try:
            for portfolio_id, positions in self.position_cache.items():
                if instrument_id in positions:
                    position = positions[instrument_id]
                    
                    # Update position with new price
                    old_market_value = position.market_value
                    position.timestamp = datetime.now(timezone.utc)
                    
                    # Calculate new market value and P&L
                    position.market_value = position.quantity * new_price
                    position.unrealized_pnl = (new_price - position.average_cost) * position.quantity
                    
                    # Log significant changes
                    if old_market_value and abs(position.market_value - old_market_value) > old_market_value * Decimal('0.01'):
                        self.logger.info(
                            f"Significant price update for portfolio {portfolio_id}, "
                            f"instrument {instrument_id}: {old_market_value} -> {position.market_value}"
                        )
            
        except Exception as e:
            self.logger.error(f"Error updating positions for instrument {instrument_id}: {str(e)}")
    
    async def _position_updater(self):
        """Periodically update positions in database"""
        while True:
            try:
                # Update positions in database every minute
                for portfolio_id in self.position_cache:
                    await self.portfolio_service.update_positions_with_market_data(portfolio_id)
                
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                self.logger.error(f"Error in position updater: {str(e)}")
                await asyncio.sleep(60)
    
    async def _performance_calculator(self):
        """Periodically calculate and update performance metrics"""
        while True:
            try:
                # Update performance metrics every 5 minutes
                for portfolio_id in self.position_cache:
                    await self.portfolio_service.update_portfolio_performance(portfolio_id)
                
                await asyncio.sleep(300)  # Update every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in performance calculator: {str(e)}")
                await asyncio.sleep(300)
    
    def add_callback(self, callback: Callable[[str, Dict[str, Any]], None]):
        """Add callback for real-time events"""
        self.callbacks.append(callback)
    
    async def _notify_callbacks(self, event_type: str, data: Dict[str, Any]):
        """Notify all registered callbacks"""
        for callback in self.callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(event_type, data)
                else:
                    callback(event_type, data)
            except Exception as e:
                self.logger.error(f"Error in callback notification: {str(e)}")
    
    async def get_real_time_positions(self, portfolio_id: int) -> List[Position]:
        """Get real-time positions from cache"""
        if portfolio_id in self.position_cache:
            return list(self.position_cache[portfolio_id].values())
        return []
    
    async def get_real_time_pnl(self, portfolio_id: int) -> Dict[str, Decimal]:
        """Get real-time P&L for a portfolio"""
        positions = await self.get_real_time_positions(portfolio_id)
        
        total_unrealized_pnl = sum(pos.unrealized_pnl or Decimal('0') for pos in positions)
        total_realized_pnl = sum(pos.realized_pnl for pos in positions)
        total_market_value = sum(pos.market_value or Decimal('0') for pos in positions)
        
        return {
            "unrealized_pnl": total_unrealized_pnl,
            "realized_pnl": total_realized_pnl,
            "total_pnl": total_unrealized_pnl + total_realized_pnl,
            "market_value": total_market_value,
            "timestamp": datetime.now(timezone.utc)
        }
    
    async def subscribe_to_symbol(self, symbol: str, portfolio_id: int):
        """Subscribe to real-time updates for a specific symbol"""
        subscription_key = f"{portfolio_id}:{symbol}"
        self.active_subscriptions[subscription_key] = True
        
        # Get instrument ID for symbol
        instrument = await self.portfolio_service.get_instrument_by_symbol(symbol)
        if instrument:
            self.logger.info(f"Subscribed to real-time updates for {symbol} in portfolio {portfolio_id}")
        else:
            self.logger.warning(f"Instrument not found for symbol {symbol}")
    
    async def unsubscribe_from_symbol(self, symbol: str, portfolio_id: int):
        """Unsubscribe from real-time updates for a specific symbol"""
        subscription_key = f"{portfolio_id}:{symbol}"
        self.active_subscriptions.pop(subscription_key, None)
        self.logger.info(f"Unsubscribed from real-time updates for {symbol} in portfolio {portfolio_id}")
    
    async def get_portfolio_summary_real_time(self, portfolio_id: int) -> Dict[str, Any]:
        """Get real-time portfolio summary"""
        try:
            positions = await self.get_real_time_positions(portfolio_id)
            pnl_data = await self.get_real_time_pnl(portfolio_id)
            
            # Calculate allocations
            total_value = pnl_data["market_value"]
            allocations = []
            
            for position in positions:
                if position.quantity != 0 and total_value > 0:
                    weight = (position.market_value or Decimal('0')) / total_value * 100
                    
                    # Get instrument details
                    instrument = await self.portfolio_service.get_instrument_by_id(position.instrument_id)
                    
                    allocations.append({
                        "symbol": instrument.symbol if instrument else "UNKNOWN",
                        "weight_percent": weight,
                        "market_value": position.market_value or Decimal('0'),
                        "unrealized_pnl": position.unrealized_pnl or Decimal('0'),
                        "quantity": position.quantity
                    })
            
            # Sort by weight
            allocations.sort(key=lambda x: x["weight_percent"], reverse=True)
            
            return {
                "portfolio_id": portfolio_id,
                "timestamp": datetime.now(timezone.utc),
                "pnl": pnl_data,
                "position_count": len([pos for pos in positions if pos.quantity != 0]),
                "top_positions": allocations[:10],  # Top 10 positions
                "total_value": total_value
            }
            
        except Exception as e:
            self.logger.error(f"Error getting real-time portfolio summary: {str(e)}")
            return {}
    
    async def stop_tracking(self):
        """Stop real-time tracking"""
        self.active_subscriptions.clear()
        self.position_cache.clear()
        self.logger.info("Stopped real-time tracking")


class WebSocketHandler:
    """
    WebSocket handler for real-time portfolio updates
    Provides WebSocket endpoints for real-time data streaming
    """
    
    def __init__(self, real_time_tracker: RealTimeTracker):
        self.tracker = real_time_tracker
        self.logger = logging.getLogger(__name__)
        self.connected_clients: Dict[str, websockets.WebSocketServerProtocol] = {}
    
    async def handle_client(self, websocket, path):
        """Handle WebSocket client connections"""
        client_id = f"client_{id(websocket)}"
        self.connected_clients[client_id] = websocket
        
        try:
            self.logger.info(f"Client {client_id} connected")
            
            async for message in websocket:
                await self._handle_client_message(client_id, message)
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            self.logger.error(f"Error handling client {client_id}: {str(e)}")
        finally:
            self.connected_clients.pop(client_id, None)
    
    async def _handle_client_message(self, client_id: str, message: str):
        """Handle incoming client messages"""
        try:
            data = json.loads(message)
            action = data.get("action")
            
            if action == "subscribe_portfolio":
                portfolio_id = data.get("portfolio_id")
                if portfolio_id:
                    await self._subscribe_portfolio(client_id, portfolio_id)
            
            elif action == "unsubscribe_portfolio":
                portfolio_id = data.get("portfolio_id")
                if portfolio_id:
                    await self._unsubscribe_portfolio(client_id, portfolio_id)
            
            elif action == "get_real_time_summary":
                portfolio_id = data.get("portfolio_id")
                if portfolio_id:
                    summary = await self.tracker.get_portfolio_summary_real_time(portfolio_id)
                    await self._send_to_client(client_id, {
                        "type": "portfolio_summary",
                        "data": summary
                    })
            
        except Exception as e:
            self.logger.error(f"Error handling message from {client_id}: {str(e)}")
    
    async def _subscribe_portfolio(self, client_id: str, portfolio_id: int):
        """Subscribe client to portfolio updates"""
        # Implementation for client subscription management
        pass
    
    async def _unsubscribe_portfolio(self, client_id: str, portfolio_id: int):
        """Unsubscribe client from portfolio updates"""
        # Implementation for client unsubscription management
        pass
    
    async def _send_to_client(self, client_id: str, data: Dict[str, Any]):
        """Send data to specific client"""
        if client_id in self.connected_clients:
            try:
                await self.connected_clients[client_id].send(json.dumps(data, default=str))
            except Exception as e:
                self.logger.error(f"Error sending data to {client_id}: {str(e)}")
    
    async def broadcast_update(self, update_type: str, data: Dict[str, Any]):
        """Broadcast update to all connected clients"""
        message = json.dumps({
            "type": update_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc)
        }, default=str)
        
        disconnected_clients = []
        for client_id, websocket in self.connected_clients.items():
            try:
                await websocket.send(message)
            except Exception as e:
                self.logger.error(f"Error broadcasting to {client_id}: {str(e)}")
                disconnected_clients.append(client_id)
        
        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.connected_clients.pop(client_id, None)