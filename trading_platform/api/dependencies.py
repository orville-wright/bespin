"""
API Dependencies

Dependency injection for FastAPI endpoints.
"""

from fastapi import Depends, HTTPException, status
from ..core import TradingEngine


async def get_trading_engine() -> TradingEngine:
    """Get the trading engine instance"""
    from .app import trading_engine
    
    if not trading_engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Trading engine not available"
        )
    
    if not trading_engine.is_running:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Trading engine not running"
        )
    
    return trading_engine


def get_current_user():
    """Get current user (placeholder for authentication)"""
    # In a real implementation, this would handle authentication
    return {"user_id": "demo_user", "permissions": ["trade", "view"]}


def require_permission(permission: str):
    """Require specific permission"""
    def permission_checker(user=Depends(get_current_user)):
        if permission not in user.get("permissions", []):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission required: {permission}"
            )
        return user
    return permission_checker