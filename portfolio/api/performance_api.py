"""
Performance Analytics API Endpoints
FastAPI router for advanced portfolio performance analysis
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse
import logging

from ..models.portfolio_models import (
    PerformanceMetrics, RiskMetrics, AttributionAnalysis,
    PerformanceAnalysis, RiskAnalysis
)
from ..services.portfolio_service import PortfolioService
from ..services.performance_engine import PerformanceEngine

router = APIRouter(prefix="/api/v1/performance", tags=["performance"])
logger = logging.getLogger(__name__)


# Dependency injection
async def get_portfolio_service() -> PortfolioService:
    """Get portfolio service instance"""
    connection_string = "postgresql://user:password@localhost:5432/trading_db"
    return PortfolioService(connection_string)


async def get_performance_engine() -> PerformanceEngine:
    """Get performance engine instance"""
    connection_string = "postgresql://user:password@localhost:5432/trading_db"
    return PerformanceEngine(connection_string)


@router.get("/portfolio/{portfolio_id}/analysis", response_model=dict)
async def get_comprehensive_analysis(
    portfolio_id: int,
    period: str = Query("1Y", description="Period: 1D, 1W, 1M, 3M, 6M, 1Y, YTD, ALL"),
    benchmark_symbol: Optional[str] = Query(None, description="Benchmark symbol for comparison"),
    service: PortfolioService = Depends(get_portfolio_service),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get comprehensive performance analysis"""
    try:
        # Calculate date range based on period
        end_date = datetime.now(timezone.utc)
        
        if period == "1D":
            start_date = end_date - timedelta(days=1)
        elif period == "1W":
            start_date = end_date - timedelta(weeks=1)
        elif period == "1M":
            start_date = end_date - timedelta(days=30)
        elif period == "3M":
            start_date = end_date - timedelta(days=90)
        elif period == "6M":
            start_date = end_date - timedelta(days=180)
        elif period == "1Y":
            start_date = end_date - timedelta(days=365)
        elif period == "YTD":
            start_date = datetime(end_date.year, 1, 1, tzinfo=timezone.utc)
        else:  # ALL
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        
        # Get comprehensive analysis
        analysis = await engine.calculate_comprehensive_performance(
            portfolio_id, start_date, end_date, benchmark_symbol
        )
        
        return {
            "portfolio_id": portfolio_id,
            "period": period,
            "analysis": analysis,
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Error getting performance analysis for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/returns", response_model=dict)
async def get_returns_analysis(
    portfolio_id: int,
    frequency: str = Query("daily", description="Return frequency: daily, weekly, monthly"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get detailed returns analysis"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        returns_data = await engine.calculate_returns_analysis(
            portfolio_id, start_date, end_date, frequency
        )
        
        return {
            "portfolio_id": portfolio_id,
            "frequency": frequency,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "returns_analysis": returns_data
        }
    except Exception as e:
        logger.error(f"Error getting returns analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/risk-metrics", response_model=dict)
async def get_advanced_risk_metrics(
    portfolio_id: int,
    confidence_levels: List[float] = Query([0.95, 0.99], description="VaR confidence levels"),
    lookback_days: int = Query(252, description="Lookback period in days"),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get advanced risk metrics"""
    try:
        risk_analysis = await engine.calculate_advanced_risk_metrics(
            portfolio_id, confidence_levels, lookback_days
        )
        
        return {
            "portfolio_id": portfolio_id,
            "lookback_days": lookback_days,
            "confidence_levels": confidence_levels,
            "risk_analysis": risk_analysis
        }
    except Exception as e:
        logger.error(f"Error calculating advanced risk metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/drawdown", response_model=dict)
async def get_drawdown_analysis(
    portfolio_id: int,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get detailed drawdown analysis"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        drawdown_data = await engine.calculate_drawdown_analysis(
            portfolio_id, start_date, end_date
        )
        
        return {
            "portfolio_id": portfolio_id,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "drawdown_analysis": drawdown_data
        }
    except Exception as e:
        logger.error(f"Error calculating drawdown analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/attribution", response_model=dict)
async def get_performance_attribution(
    portfolio_id: int,
    attribution_type: str = Query("brinson", description="Attribution method: brinson, returns_based"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    benchmark_id: Optional[int] = Query(None),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get detailed performance attribution analysis"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=90)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        attribution_data = await engine.calculate_performance_attribution(
            portfolio_id, start_date, end_date, attribution_type, benchmark_id
        )
        
        return {
            "portfolio_id": portfolio_id,
            "attribution_method": attribution_type,
            "benchmark_id": benchmark_id,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "attribution": attribution_data
        }
    except Exception as e:
        logger.error(f"Error calculating performance attribution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/benchmark-comparison", response_model=dict)
async def get_benchmark_comparison(
    portfolio_id: int,
    benchmark_symbols: List[str] = Query(..., description="List of benchmark symbols"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Compare portfolio performance against multiple benchmarks"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        comparison_data = await engine.compare_with_benchmarks(
            portfolio_id, benchmark_symbols, start_date, end_date
        )
        
        return {
            "portfolio_id": portfolio_id,
            "benchmarks": benchmark_symbols,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "comparison": comparison_data
        }
    except Exception as e:
        logger.error(f"Error calculating benchmark comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/rolling-metrics", response_model=dict)
async def get_rolling_metrics(
    portfolio_id: int,
    window_days: int = Query(30, description="Rolling window in days"),
    metrics: List[str] = Query(["returns", "volatility", "sharpe"], description="Metrics to calculate"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Get rolling performance metrics"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        rolling_data = await engine.calculate_rolling_metrics(
            portfolio_id, window_days, metrics, start_date, end_date
        )
        
        return {
            "portfolio_id": portfolio_id,
            "window_days": window_days,
            "metrics": metrics,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "rolling_metrics": rolling_data
        }
    except Exception as e:
        logger.error(f"Error calculating rolling metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portfolio/{portfolio_id}/performance-report", response_model=dict)
async def generate_performance_report(
    portfolio_id: int,
    report_type: str = Query("comprehensive", description="Report type: summary, comprehensive, risk_focused"),
    period: str = Query("1Y", description="Report period"),
    background_tasks: BackgroundTasks,
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Generate a comprehensive performance report"""
    try:
        # Generate report in background
        report_id = f"report_{portfolio_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        background_tasks.add_task(
            engine.generate_performance_report,
            portfolio_id, report_type, period, report_id
        )
        
        return {
            "message": "Performance report generation initiated",
            "report_id": report_id,
            "portfolio_id": portfolio_id,
            "status": "processing"
        }
    except Exception as e:
        logger.error(f"Error initiating performance report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/stress-test", response_model=dict)
async def run_stress_test(
    portfolio_id: int,
    scenario: str = Query("market_crash", description="Stress test scenario"),
    shock_magnitude: float = Query(-0.2, description="Shock magnitude (e.g., -0.2 for -20%)"),
    engine: PerformanceEngine = Depends(get_performance_engine)
):
    """Run stress test scenarios on portfolio"""
    try:
        stress_results = await engine.run_stress_test(
            portfolio_id, scenario, shock_magnitude
        )
        
        return {
            "portfolio_id": portfolio_id,
            "scenario": scenario,
            "shock_magnitude": shock_magnitude,
            "stress_test_results": stress_results,
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Error running stress test: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=dict)
async def health_check():
    """Performance API health check"""
    return {
        "status": "healthy",
        "service": "performance_api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }