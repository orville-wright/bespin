"""
Integration with existing Bespin infrastructure

Provides integration points with aop.py orchestrator and existing data engines.
"""

from .aop_integration import AOPIntegration
from .data_engine_bridge import DataEngineBridge

__all__ = [
    "AOPIntegration",
    "DataEngineBridge",
]