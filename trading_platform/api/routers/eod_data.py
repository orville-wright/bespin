"""
EOD Historical Data API Router

REST endpoints for accessing EOD Historical Data through the trading platform.
Provides unified access to EOD's market data, fundamentals, and technical indicators.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse

from ...core import TradingEngine
from ..dependencies import get_trading_engine, require_permission

router = APIRouter()


@router.get("/quote/{symbol}")
async def get_eod_quote(
    symbol: str,
    exchange: str = Query("US", description="Exchange code (US, LSE, XETRA, etc.)"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get real-time quote from EOD Historical Data"""
    try:
        # Get EOD integration from market data handler
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        quote = await eod_source.get_quote(symbol, exchange)
        
        if not quote:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No quote data available for {symbol}.{exchange}"
            )
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "quote": quote,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD quote: {str(e)}"
        )


@router.get("/historical/{symbol}")
async def get_eod_historical(
    symbol: str,
    exchange: str = Query("US", description="Exchange code"),
    limit: int = Query(100, ge=1, le=1000, description="Number of data points"),
    timeframe: str = Query("1day", description="Timeframe (1day, 1week, 1month)"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get historical data from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        historical_data = await eod_source.get_historical_data(symbol, timeframe, limit, exchange)
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "timeframe": timeframe,
            "data_points": len(historical_data),
            "data": historical_data,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD historical data: {str(e)}"
        )


@router.get("/intraday/{symbol}")
async def get_eod_intraday(
    symbol: str,
    exchange: str = Query("US", description="Exchange code"),
    interval: str = Query("5m", description="Interval (1m, 5m, 1h)"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get intraday data from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        intraday_data = await eod_source.get_intraday_data(symbol, interval, exchange)
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "interval": interval,
            "data_points": len(intraday_data),
            "data": intraday_data,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD intraday data: {str(e)}"
        )


@router.get("/fundamentals/{symbol}")
async def get_eod_fundamentals(
    symbol: str,
    exchange: str = Query("US", description="Exchange code"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get fundamental data from EOD Historical Data (consumes 10 API calls)"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        fundamentals = await eod_source.get_fundamentals(symbol, exchange)
        
        if not fundamentals:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No fundamental data available for {symbol}.{exchange}"
            )
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "fundamentals": fundamentals,
            "source": "eod_historical_data",
            "api_cost": 10
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD fundamentals: {str(e)}"
        )


@router.get("/technical/{symbol}")
async def get_eod_technical(
    symbol: str,
    function: str = Query("sma", description="Technical indicator function"),
    period: int = Query(50, ge=1, le=200, description="Period for calculation"),
    exchange: str = Query("US", description="Exchange code"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get technical indicators from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        technical_data = await eod_source.get_technical_indicators(symbol, function, period, exchange)
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "function": function,
            "period": period,
            "data_points": len(technical_data),
            "data": technical_data,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD technical indicators: {str(e)}"
        )


@router.get("/dividends/{symbol}")
async def get_eod_dividends(
    symbol: str,
    exchange: str = Query("US", description="Exchange code"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get dividend data from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        dividend_data = await eod_source.get_dividend_data(symbol, exchange)
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "dividend_records": len(dividend_data),
            "data": dividend_data,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD dividend data: {str(e)}"
        )


@router.get("/exchanges")
async def get_eod_exchanges(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get list of supported exchanges from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        exchanges = await eod_source.get_supported_exchanges()
        
        return {
            "exchanges_count": len(exchanges),
            "exchanges": exchanges,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD exchanges: {str(e)}"
        )


@router.get("/bulk-quotes")
async def get_eod_bulk_quotes(
    symbols: str = Query(..., description="Comma-separated symbols"),
    exchange: str = Query("US", description="Exchange code"),
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get bulk quotes from EOD Historical Data"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
        
        if len(symbol_list) > 10:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 10 symbols allowed per request"
            )
        
        quotes = await eod_source.get_bulk_quotes(symbol_list, exchange)
        
        return {
            "symbols_requested": len(symbol_list),
            "symbols_found": len([q for q in quotes.values() if 'error' not in q]),
            "quotes": quotes,
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD bulk quotes: {str(e)}"
        )


@router.get("/usage")
async def get_eod_usage(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get EOD API usage statistics"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        usage = eod_source.get_api_usage()
        
        return {
            "api_usage": usage,
            "supported_data_types": eod_source.get_supported_data_types(),
            "source": "eod_historical_data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting EOD usage: {str(e)}"
        )


@router.get("/test")
async def test_eod_connection(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Test EOD Historical Data connection"""
    try:
        eod_source = trading_engine.market_data_handler.data_sources.get('eod')
        
        if not eod_source:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="EOD Historical Data integration not available"
            )
        
        test_result = await eod_source.test_connection()
        
        return test_result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error testing EOD connection: {str(e)}"
        )