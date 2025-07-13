"""
Bespin Trading Platform

A comprehensive quantitative trading platform built on FastAPI with PostgreSQL+TimescaleDB,
Redis, and AsyncIO for high-performance trading strategy execution.

Integrates with existing Bespin data infrastructure (aop.py orchestrator) and data engines.
"""

__version__ = "1.0.0"
__author__ = "Bespin Trading Platform Team"

from .core import TradingEngine
from .models import Position, Order, Strategy
from .api import create_app

__all__ = [
    "TradingEngine",
    "Position", 
    "Order",
    "Strategy",
    "create_app",
]