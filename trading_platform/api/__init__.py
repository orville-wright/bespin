"""
Trading Platform API

FastAPI-based REST API for trading platform operations.
Provides endpoints for strategies, orders, positions, and market data.
"""

from .app import create_app
from .routers import strategies, orders, positions, market_data

__all__ = [
    "create_app",
    "strategies", 
    "orders",
    "positions",
    "market_data",
]