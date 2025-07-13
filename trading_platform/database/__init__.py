"""
Database integration for the trading platform

Provides database schemas and connection management for PostgreSQL + TimescaleDB.
"""

from .schemas import create_tables, drop_tables
from .connection import get_database_connection, DatabaseManager

__all__ = [
    "create_tables",
    "drop_tables", 
    "get_database_connection",
    "DatabaseManager",
]