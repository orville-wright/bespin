"""
Position Manager

Manages position lifecycle, P&L calculations, and position tracking.
Integrates with order fills to maintain accurate position state.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import uuid

from ...models import Position, PositionCreate, PositionUpdate, PositionStatus, Order

logger = logging.getLogger(__name__)


class PositionManager:
    """
    Manages all position operations and tracking
    
    Responsibilities:
    - Position creation from order fills
    - P&L calculation and tracking
    - Position updates and market value tracking
    - Position closing and realization
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Position storage (in production, this would be database-backed)
        self.positions: Dict[str, Position] = {}
        self.positions_by_symbol: Dict[str, List[Position]] = {}
        self.positions_by_strategy: Dict[str, List[Position]] = {}
        
        # Performance tracking
        self.stats = {
            'positions_opened': 0,
            'positions_closed': 0,
            'total_realized_pnl': Decimal('0'),
            'total_unrealized_pnl': Decimal('0'),
            'total_fees': Decimal('0'),
            'winning_positions': 0,
            'losing_positions': 0
        }
        
        logger.info(f"PositionManager initialized (simulation: {self.is_simulation})")
    
    async def initialize(self) -> None:
        """Initialize the position manager"""
        try:
            # Load existing positions if any (from database in production)
            await self._load_positions()
            logger.info("PositionManager initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing PositionManager: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the position manager"""
        try:
            # Save positions if needed
            await self._save_positions()
            logger.info("PositionManager stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping PositionManager: {e}")
    
    async def _load_positions(self) -> None:
        """Load existing positions from storage"""
        # In production, this would load from database
        # For now, start with empty positions
        pass
    
    async def _save_positions(self) -> None:
        """Save positions to storage"""
        # In production, this would save to database
        pass
    
    async def handle_order_fill(self, order: Order, fill_data: Dict[str, Any]) -> Optional[Position]:
        """Handle order fill and update/create positions"""
        try:
            symbol = order.symbol
            strategy_id = order.strategy_id
            
            # Check if we have existing position for this symbol/strategy
            existing_position = await self._find_position(symbol, strategy_id)
            
            if existing_position:
                # Update existing position
                position = await self._update_position_from_fill(existing_position, order, fill_data)
            else:
                # Create new position
                position = await self._create_position_from_fill(order, fill_data)
            
            if position:
                self._update_position_indexes(position)
                fill_data['position_opened'] = existing_position is None
                fill_data['position_closed'] = position.status == PositionStatus.CLOSED
            
            return position
            
        except Exception as e:
            logger.error(f"Error handling order fill: {e}")
            return None
    
    async def _create_position_from_fill(self, order: Order, fill_data: Dict[str, Any]) -> Optional[Position]:
        """Create new position from order fill"""
        try:
            position_id = str(uuid.uuid4())
            
            # Calculate position details
            quantity = order.filled_quantity
            if order.side == "sell":
                quantity = -quantity  # Negative for short positions
            
            avg_entry_price = order.avg_fill_price or Decimal('0')
            cost_basis = abs(quantity) * avg_entry_price
            
            # Create position
            position = Position(
                position_id=position_id,
                strategy_id=order.strategy_id,
                symbol=order.symbol,
                status=PositionStatus.OPEN,
                quantity=quantity,
                avg_entry_price=avg_entry_price,
                cost_basis=cost_basis,
                opened_at=order.filled_at or datetime.utcnow(),
                is_simulation=order.is_simulation
            )
            
            # Store position
            self.positions[position_id] = position
            self.stats['positions_opened'] += 1
            
            logger.info(f"Position created: {position_id} ({order.symbol} {quantity})")
            
            return position
            
        except Exception as e:
            logger.error(f"Error creating position from fill: {e}")
            return None
    
    async def _update_position_from_fill(self, position: Position, order: Order, fill_data: Dict[str, Any]) -> Optional[Position]:
        """Update existing position from order fill"""
        try:
            # Calculate new position size
            fill_quantity = order.filled_quantity
            if order.side == "sell":
                fill_quantity = -fill_quantity
            
            new_quantity = position.quantity + fill_quantity
            
            if new_quantity == 0:
                # Position closed
                await self._close_position(position, order)
                return position
            elif (position.quantity > 0 and new_quantity < 0) or (position.quantity < 0 and new_quantity > 0):
                # Position flipped direction - close old and create new
                await self._close_position(position, order)
                
                # Create new position with remaining quantity
                remaining_order = Order(
                    order_id=f"{order.order_id}_remaining",
                    strategy_id=order.strategy_id,
                    symbol=order.symbol,
                    side=order.side,
                    order_type=order.order_type,
                    quantity=abs(new_quantity),
                    filled_quantity=abs(new_quantity),
                    avg_fill_price=order.avg_fill_price,
                    filled_at=order.filled_at,
                    is_simulation=order.is_simulation
                )
                
                return await self._create_position_from_fill(remaining_order, fill_data)
            else:
                # Update existing position
                old_quantity = abs(position.quantity)
                old_cost_basis = position.cost_basis
                
                # Calculate new average entry price
                fill_cost = abs(fill_quantity) * (order.avg_fill_price or Decimal('0'))
                new_cost_basis = old_cost_basis + fill_cost
                new_avg_entry_price = new_cost_basis / abs(new_quantity)
                
                # Update position
                position.quantity = new_quantity
                position.avg_entry_price = new_avg_entry_price
                position.cost_basis = new_cost_basis
                
                logger.info(f"Position updated: {position.position_id} ({new_quantity})")
                
                return position
                
        except Exception as e:
            logger.error(f"Error updating position from fill: {e}")
            return None
    
    async def _close_position(self, position: Position, closing_order: Order) -> None:
        """Close a position"""
        try:
            close_price = closing_order.avg_fill_price or Decimal('0')
            
            # Calculate P&L
            pnl = (close_price - position.avg_entry_price) * position.quantity
            
            # Update position
            position.status = PositionStatus.CLOSED
            position.closed_at = closing_order.filled_at or datetime.utcnow()
            position.current_price = close_price
            position.realized_pnl = pnl
            position.unrealized_pnl = Decimal('0')
            position.total_pnl = pnl
            
            # Update stats
            self.stats['positions_closed'] += 1
            self.stats['total_realized_pnl'] += pnl
            
            if pnl > 0:
                self.stats['winning_positions'] += 1
            else:
                self.stats['losing_positions'] += 1
            
            logger.info(f"Position closed: {position.position_id} (P&L: {pnl})")
            
        except Exception as e:
            logger.error(f"Error closing position: {e}")
    
    async def _find_position(self, symbol: str, strategy_id: str) -> Optional[Position]:
        """Find existing open position for symbol/strategy"""
        for position in self.positions.values():
            if (position.symbol == symbol and 
                position.strategy_id == strategy_id and 
                position.status == PositionStatus.OPEN):
                return position
        return None
    
    def _update_position_indexes(self, position: Position) -> None:
        """Update position indexes for fast lookup"""
        # By symbol
        if position.symbol not in self.positions_by_symbol:
            self.positions_by_symbol[position.symbol] = []
        if position not in self.positions_by_symbol[position.symbol]:
            self.positions_by_symbol[position.symbol].append(position)
        
        # By strategy
        if position.strategy_id not in self.positions_by_strategy:
            self.positions_by_strategy[position.strategy_id] = []
        if position not in self.positions_by_strategy[position.strategy_id]:
            self.positions_by_strategy[position.strategy_id].append(position)
    
    async def update_position_prices(self, symbol: str, current_price: Decimal) -> None:
        """Update position prices and unrealized P&L"""
        try:
            if symbol in self.positions_by_symbol:
                for position in self.positions_by_symbol[symbol]:
                    if position.status == PositionStatus.OPEN:
                        position.update_current_price(current_price)
                        
        except Exception as e:
            logger.error(f"Error updating position prices for {symbol}: {e}")
    
    async def update_all_positions(self) -> None:
        """Update all position valuations"""
        try:
            # In a real implementation, this would fetch current prices
            # from market data and update all positions
            
            total_unrealized = Decimal('0')
            
            for position in self.positions.values():
                if position.status == PositionStatus.OPEN:
                    total_unrealized += position.unrealized_pnl
            
            self.stats['total_unrealized_pnl'] = total_unrealized
            
        except Exception as e:
            logger.error(f"Error updating all positions: {e}")
    
    # Public API methods
    
    async def get_positions(self, strategy_id: str = None, symbol: str = None, status: str = None) -> List[Position]:
        """Get positions with optional filtering"""
        positions = list(self.positions.values())
        
        if strategy_id:
            positions = [p for p in positions if p.strategy_id == strategy_id]
        
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        
        if status:
            positions = [p for p in positions if p.status == status]
        
        return positions
    
    async def get_position(self, position_id: str) -> Optional[Position]:
        """Get position by ID"""
        return self.positions.get(position_id)
    
    async def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        return [p for p in self.positions.values() if p.status == PositionStatus.OPEN]
    
    async def create_position(self, position_data: PositionCreate) -> Optional[Position]:
        """Create a manual position"""
        try:
            position_id = str(uuid.uuid4())
            
            position = Position(
                position_id=position_id,
                strategy_id=position_data.strategy_id,
                symbol=position_data.symbol,
                quantity=position_data.quantity,
                avg_entry_price=position_data.avg_entry_price,
                cost_basis=position_data.cost_basis,
                opened_at=datetime.utcnow(),
                stop_loss=position_data.stop_loss,
                take_profit=position_data.take_profit,
                metadata=position_data.metadata,
                is_simulation=position_data.is_simulation or self.is_simulation
            )
            
            self.positions[position_id] = position
            self._update_position_indexes(position)
            self.stats['positions_opened'] += 1
            
            logger.info(f"Manual position created: {position_id}")
            
            return position
            
        except Exception as e:
            logger.error(f"Error creating manual position: {e}")
            return None
    
    async def close_position_manual(self, position_id: str, close_price: Decimal) -> bool:
        """Manually close a position"""
        try:
            position = self.positions.get(position_id)
            if not position:
                logger.error(f"Position not found: {position_id}")
                return False
            
            if position.status != PositionStatus.OPEN:
                logger.error(f"Position is not open: {position_id}")
                return False
            
            # Close position
            position.close_position(close_price)
            
            # Update stats
            self.stats['positions_closed'] += 1
            self.stats['total_realized_pnl'] += position.realized_pnl
            
            if position.realized_pnl > 0:
                self.stats['winning_positions'] += 1
            else:
                self.stats['losing_positions'] += 1
            
            logger.info(f"Position manually closed: {position_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error closing position manually: {e}")
            return False
    
    def get_total_pnl(self) -> Decimal:
        """Get total P&L across all positions"""
        return self.stats['total_realized_pnl'] + self.stats['total_unrealized_pnl']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get position manager statistics"""
        stats = self.stats.copy()
        stats['total_pnl'] = self.get_total_pnl()
        stats['open_positions'] = len([p for p in self.positions.values() if p.status == PositionStatus.OPEN])
        stats['win_rate'] = (
            self.stats['winning_positions'] / max(self.stats['positions_closed'], 1) * 100
            if self.stats['positions_closed'] > 0 else 0
        )
        return stats