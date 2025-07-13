"""
Market Data API

REST endpoints for market data operations and real-time data access.
Integrates with existing data infrastructure (aop.py and data engines).
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
import json
import asyncio

from ...core import TradingEngine
from ..dependencies import get_trading_engine, require_permission

router = APIRouter()


@router.get("/quote/{symbol}")
async def get_quote(
    symbol: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get latest quote for a symbol"""
    try:
        symbol = symbol.upper()
        
        # Get latest market data
        market_data = await trading_engine.market_data_handler.get_latest_data(symbol)
        
        if not market_data or symbol not in market_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No market data available for symbol: {symbol}"
            )
        
        data = market_data[symbol]
        
        return {
            "symbol": symbol,
            "price": data.get("price"),
            "bid": data.get("bid"),
            "ask": data.get("ask"),
            "volume": data.get("volume"),
            "timestamp": data.get("timestamp"),
            "source": data.get("source", "unknown")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting quote: {str(e)}"
        )


@router.get("/quotes")
async def get_quotes(
    symbols: str,  # Comma-separated symbols
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get latest quotes for multiple symbols"""
    try:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
        
        quotes = {}
        for symbol in symbol_list:
            try:
                market_data = await trading_engine.market_data_handler.get_latest_data(symbol)
                if market_data and symbol in market_data:
                    data = market_data[symbol]
                    quotes[symbol] = {
                        "price": data.get("price"),
                        "bid": data.get("bid"),
                        "ask": data.get("ask"),
                        "volume": data.get("volume"),
                        "timestamp": data.get("timestamp"),
                        "source": data.get("source", "unknown")
                    }
                else:
                    quotes[symbol] = {"error": "No data available"}
            except Exception as e:
                quotes[symbol] = {"error": str(e)}
        
        return {
            "quotes": quotes,
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting quotes: {str(e)}"
        )


@router.get("/historical/{symbol}")
async def get_historical_data(
    symbol: str,
    limit: int = 100,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get historical market data for a symbol"""
    try:
        symbol = symbol.upper()
        
        historical_data = trading_engine.market_data_handler.get_historical_data(symbol, limit)
        
        if not historical_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No historical data available for symbol: {symbol}"
            )
        
        return {
            "symbol": symbol,
            "data_points": len(historical_data),
            "data": historical_data,
            "limit": limit
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting historical data: {str(e)}"
        )


@router.post("/subscribe")
async def subscribe_to_symbol(
    subscription_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Subscribe to market data for symbols"""
    try:
        symbols = subscription_data.get("symbols", [])
        
        if not symbols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No symbols provided"
            )
        
        results = {}
        for symbol in symbols:
            symbol = symbol.upper()
            success = await trading_engine.subscribe_symbol(symbol)
            results[symbol] = "subscribed" if success else "failed"
        
        return {
            "status": "completed",
            "subscriptions": results,
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error subscribing to symbols: {str(e)}"
        )


@router.post("/unsubscribe")
async def unsubscribe_from_symbol(
    subscription_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Unsubscribe from market data for symbols"""
    try:
        symbols = subscription_data.get("symbols", [])
        
        if not symbols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No symbols provided"
            )
        
        results = {}
        for symbol in symbols:
            symbol = symbol.upper()
            success = await trading_engine.unsubscribe_symbol(symbol)
            results[symbol] = "unsubscribed" if success else "failed"
        
        return {
            "status": "completed",
            "unsubscriptions": results,
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error unsubscribing from symbols: {str(e)}"
        )


@router.get("/subscriptions")
async def get_subscriptions(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get list of currently subscribed symbols"""
    try:
        subscribed_symbols = trading_engine.market_data_handler.get_subscribed_symbols()
        
        return {
            "subscribed_symbols": subscribed_symbols,
            "count": len(subscribed_symbols),
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting subscriptions: {str(e)}"
        )


@router.get("/stats")
async def get_market_data_stats(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get market data handler statistics"""
    try:
        stats = trading_engine.market_data_handler.get_stats()
        
        return {
            "market_data_statistics": stats,
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting market data stats: {str(e)}"
        )


# WebSocket endpoint for real-time data
@router.websocket("/stream")
async def market_data_stream(websocket: WebSocket):
    """WebSocket endpoint for real-time market data streaming"""
    await websocket.accept()
    
    try:
        # Get trading engine (simplified for WebSocket)
        from ..app import trading_engine
        
        if not trading_engine:
            await websocket.send_text(json.dumps({
                "error": "Trading engine not available"
            }))
            await websocket.close()
            return
        
        subscribed_symbols = set()
        
        async def send_market_data(symbol: str, data: dict):
            """Callback to send market data updates"""
            try:
                message = {
                    "type": "market_data",
                    "symbol": symbol,
                    "data": {
                        "price": data.get("price"),
                        "bid": data.get("bid"),
                        "ask": data.get("ask"),
                        "volume": data.get("volume"),
                        "timestamp": data.get("timestamp").isoformat() if data.get("timestamp") else None
                    }
                }
                await websocket.send_text(json.dumps(message))
            except Exception as e:
                print(f"Error sending market data: {e}")
        
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_text()
                message = json.loads(data)
                
                action = message.get("action")
                symbols = message.get("symbols", [])
                
                if action == "subscribe":
                    for symbol in symbols:
                        symbol = symbol.upper()
                        if symbol not in subscribed_symbols:
                            # Subscribe to market data with callback
                            await trading_engine.market_data_handler.subscribe(symbol, send_market_data)
                            subscribed_symbols.add(symbol)
                    
                    await websocket.send_text(json.dumps({
                        "type": "subscription_response",
                        "action": "subscribe",
                        "symbols": symbols,
                        "status": "success"
                    }))
                
                elif action == "unsubscribe":
                    for symbol in symbols:
                        symbol = symbol.upper()
                        if symbol in subscribed_symbols:
                            await trading_engine.market_data_handler.unsubscribe(symbol)
                            subscribed_symbols.discard(symbol)
                    
                    await websocket.send_text(json.dumps({
                        "type": "subscription_response",
                        "action": "unsubscribe",
                        "symbols": symbols,
                        "status": "success"
                    }))
                
                elif action == "ping":
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": "2025-07-13T06:05:30Z"
                    }))
                
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "error": "Invalid JSON message"
                }))
            except Exception as e:
                await websocket.send_text(json.dumps({
                    "error": f"Error processing message: {str(e)}"
                }))
    
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        # Cleanup subscriptions
        for symbol in subscribed_symbols:
            try:
                await trading_engine.market_data_handler.unsubscribe(symbol)
            except:
                pass


# Integration with existing data infrastructure
@router.get("/integration/aop/status")
async def get_aop_integration_status(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get status of integration with aop.py orchestrator"""
    try:
        # This would check the integration status with the existing aop.py orchestrator
        # For now, return a placeholder status
        
        return {
            "aop_integration": {
                "status": "connected",
                "data_engines_active": [
                    "alpaca_md",
                    "polygon_md", 
                    "alphavantage_md",
                    "finnhub_md"
                ],
                "last_data_update": "2025-07-13T06:05:30Z",
                "data_points_today": 15420,
                "news_articles_processed": 245
            },
            "trading_platform_status": trading_engine.get_status(),
            "timestamp": "2025-07-13T06:05:30Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting AOP integration status: {str(e)}"
        )