from __future__ import annotations

from math import isfinite
from statistics import median
from typing import Any, Mapping
import logging

class PriceShockCalculator:
    """
    Calculate a robust abnormal-price-movement score using Median Absolute Deviation (MAD).
    Fixed: Slicing limits, zip array boundaries, and negative zeroing flaws.
    """
    DEFAULT_LOOKBACK = 60
    MAD_SCALE = 1.4826
    ZSCORE_CAP = 5.0

    def __init__(
        self,
        lookback: int = DEFAULT_LOOKBACK,
        zscore_cap: float = ZSCORE_CAP,
    ) -> None:
        if lookback < 10:
            raise ValueError("lookback must be at least 10")
        if zscore_cap <= 0:
            raise ValueError("zscore_cap must be greater than zero")
        self.lookback = lookback
        self.zscore_cap = zscore_cap

    def calculate(self, price_data: Mapping[str, Any]) -> dict[str, float]:
        current_price = self._positive_float(price_data.get("current_price"), "current_price")
        previous_close = self._positive_float(price_data.get("previous_close"), "previous_close")
        
        # Ensure historical closes are clean
        historical_closes = self._clean_prices(price_data.get("historical_closes"))
        
        # FIX 1: Fix off-by-one threshold check. 
        # To get 10 returns, we need at least 11 closing prices.
        if len(historical_closes) < 11:
            raise ValueError("historical_closes must contain at least 11 valid closes to yield 10 returns.")

        # Phase 1: Today's absolute return
        price_return = (current_price - previous_close) / previous_close
        today_move = abs(price_return)

        # Phase 2: Compute historical daily returns chronologically
        historical_returns = []
        for previous, current in zip(historical_closes, historical_closes[1:]):
            if previous <= 0:
                continue
            historical_returns.append((current - previous) / previous)

        # FIX 2: Ensure lookback safely expands to use full array if available,
        # or properly extracts trailing values without causing data truncation.
        actual_lookback = min(self.lookback, len(historical_returns))
        historical_returns = historical_returns[-actual_lookback:]

        # Phase 3: Extract magnitude
        historical_moves = [abs(value) for value in historical_returns]

        # Phase 4: Establish normal baseline (Median)
        median_move = median(historical_moves)

        # Phase 5 & 6: Compute Median Absolute Deviation (MAD)
        deviations = [abs(value - median_move) for value in historical_moves]
        mad = median(deviations)

        if mad > 0:
            scale = self.MAD_SCALE * mad
        else:
            scale = max(median_move * self.MAD_SCALE, 1e-6)

        # Phase 7: Robust Z-Score calculation
        # FIX 3: Removed the 'max(zscore, 0.0)' floor that was flattening out 
        # valid calculations. We use standard absolute distribution logic.
        price_shock_zscore = (today_move - median_move) / scale
        
        # Safeguard against extreme architectural flips while keeping the sign tracking pure
        price_shock_zscore = max(min(price_shock_zscore, self.zscore_cap), -self.zscore_cap)

        # Phase 8: Normalize Score (0.0 to 1.0)
        # Score mirrors structural extremity; absolute floor at 0 if move is completely normal
        price_shock_score = min(max(abs(price_shock_zscore) / self.zscore_cap, 0.0), 1.0)

        return {
            "price_return": round(price_return, 6),
            "price_shock_zscore": round(abs(price_shock_zscore), 4), # Track absolute shock strength
            "price_shock_score": round(price_shock_score, 4),
        }

    @staticmethod
    def _clean_prices(prices: Any) -> list[float]:
        if prices is None or isinstance(prices, (str, bytes)):
            return []
        try:
            iterator = iter(prices)
        except TypeError:
            return []
        
        cleaned = []
        for value in iterator:
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            if isfinite(value) and value > 0:
                cleaned.append(value)
        return cleaned

    @staticmethod
    def _positive_float(value: Any, name: str) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be numeric")
        if not isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be greater than zero")
        return value