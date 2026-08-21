from __future__ import annotations

import math
import statistics

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional


# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_MIN_EFFECTIVE_VOLUME = 3.0

DEFAULT_PRICE_SHOCK_SIGMA = 3.0

DEFAULT_COVERAGE_EXPONENT = 1.5

DEFAULT_MAX_VOLUME_FACTOR = 1.5

DEFAULT_NEWS_NEUTRAL_THRESHOLD = 0.10

DEFAULT_WATCH_THRESHOLD = 0.25
DEFAULT_MODERATE_THRESHOLD = 0.45
DEFAULT_STRONG_THRESHOLD = 0.65
DEFAULT_MAJOR_THRESHOLD = 0.80
DEFAULT_EXTREME_THRESHOLD = 0.90

# ########################################################
"""
divergence_engine.py

Standalone PRICE SHOCK -> NEWS COVERAGE GAP detection engine.
This module is intentionally independent of the news sentiment pipeline.

Responsibilities
----------------
The news sentiment pipeline answers:

    "What does the news say?"

This module answers:

    "Does the current market behavior make sense given
     the information currently available in the news pipeline?"

The engine consumes:

NEWS:
    - net_score
    - composite_score
    - n_eff
    - polarity
    - directional_density
    - signal_purity

MARKET:
    - current price
    - previous close
    - historical returns
    - current volume
    - historical volumes

It produces:

    - price shock
    - news coverage score
    - news coverage gap
    - news alignment
    - volume shock
    - divergence score
    - divergence type
    - divergence severity
    - diagnostic interpretation

Core divergence formula
-----------------------

    DAS =
        PSS
        * NCG^1.5
        * (1 - NAS)
        * VSF

Where:

    PSS = normalized price shock
    NCG = news coverage gap
    NAS = news alignment score
    VSF = volume shock factor

Final DAS is bounded to [0, 1].

"""

# ============================================================================
# ENUMS
# ============================================================================

class DivergenceSeverity(str, Enum):
    NONE = "None"
    WATCH = "Watch"
    MODERATE = "Moderate Divergence"
    STRONG = "Strong Divergence"
    MAJOR = "Major Divergence"
    EXTREME = "Extreme Divergence"


class DivergenceType(str, Enum):
    NONE = "None"
    BULLISH_PRICE_NEWS_VACUUM = ("Bullish Price / News Vacuum")
    BULLISH_PRICE_BEARISH_NEWS = ("Bullish Price / Bearish News")
    BEARISH_PRICE_NEWS_VACUUM = ("Bearish Price / News Vacuum")
    BEARISH_PRICE_BULLISH_NEWS = ("Bearish Price / Bullish News")
    BULLISH_CONFIRMATION = ("Bullish Price / Bullish News Confirmation")
    BEARISH_CONFIRMATION = ("Bearish Price / Bearish News Confirmation")

# ============================================================================
# NEWS METRICS
# ============================================================================

@dataclass(frozen=True)
class NewsMetrics:
    """
    Clean interface between the news sentiment pipeline and
    the divergence engine.

    These values should come from your completed news sentiment
    pipeline / LMDB.

    Attributes
    ----------
    net_score:
        Overall directional sentiment score.

        Example:
            +0.291

    composite_score:
        Freshness-weighted current sentiment.

        Example:
            +0.0059

    n_eff:
        Effective current news volume after temporal decay.

        Example:
            0.04

    polarity:
        Overall polarity / confidence of the news signal.

    directional_density:
        Degree to which the news corpus contains directional
        rather than neutral information.

    signal_purity:
        Dominant signal share from the sentiment engine.
    """

    net_score: float
    composite_score: float
    n_eff: float

    polarity: Optional[float] = None
    directional_density: Optional[float] = None
    signal_purity: Optional[float] = None

    @classmethod
    def from_mapping(
        cls,
        data: Mapping[str, Any],
    ) -> "NewsMetrics":
        """
        Create NewsMetrics from a dictionary.

        This is useful when loading a sentiment record from LMDB
        or JSON.
        """

        return cls(
            net_score=float(
                data.get("net_score", 0.0)
            ),

            composite_score=float(
                data.get("composite_score", 0.0)
            ),

            n_eff=float(
                data.get("n_eff", 0.0)
            ),

            polarity=(
                float(data["polarity"])
                if data.get("polarity") is not None
                else None
            ),

            directional_density=(
                float(data["directional_density"])
                if data.get("directional_density") is not None
                else None
            ),

            signal_purity=(
                float(data["signal_purity"])
                if data.get("signal_purity") is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ============================================================================
# MARKET METRICS
# ============================================================================

@dataclass(frozen=True)
class MarketMetrics:
    """
    Market information required by the divergence engine.

    Returns are expected as decimal values.

    Example:

        +13.63% = 0.1363
    """

    current_price: float
    previous_close: float

    historical_returns: list[float]

    current_volume: Optional[float] = None
    historical_volumes: Optional[list[float]] = None

    # Optional intraday returns.
    #
    # These allow the engine to identify a sudden intraday
    # price shock rather than relying only on the daily return.
    return_15m: Optional[float] = None
    return_1h: Optional[float] = None
    return_4h: Optional[float] = None

    @classmethod
    def from_mapping(
        cls,
        data: Mapping[str, Any],
    ) -> "MarketMetrics":

        return cls(
            current_price=float(
                data["current_price"]
            ),

            previous_close=float(
                data["previous_close"]
            ),

            historical_returns=[
                float(x)
                for x in data.get(
                    "historical_returns",
                    [],
                )
            ],

            current_volume=(
                float(data["current_volume"])
                if data.get("current_volume") is not None
                else None
            ),

            historical_volumes=(
                [
                    float(x)
                    for x in data["historical_volumes"]
                ]
                if data.get("historical_volumes") is not None
                else None
            ),

            return_15m=(
                float(data["return_15m"])
                if data.get("return_15m") is not None
                else None
            ),

            return_1h=(
                float(data["return_1h"])
                if data.get("return_1h") is not None
                else None
            ),

            return_4h=(
                float(data["return_4h"])
                if data.get("return_4h") is not None
                else None
            ),
        )


# ============================================================================
# PRICE SHOCK METRICS
# ============================================================================

@dataclass(frozen=True)
class PriceShockMetrics:
    """
    Calculated price shock information.
    """

    daily_return: float

    return_std: float

    price_shock_zscore: float

    price_shock_score: float

    intraday_shock_score: float

    final_price_shock_score: float

    direction: str


# ============================================================================
# VOLUME METRICS
# ============================================================================

@dataclass(frozen=True)
class VolumeShockMetrics:
    """
    Calculated volume shock information.
    """

    current_volume: Optional[float]

    average_volume: Optional[float]

    volume_ratio: Optional[float]

    volume_shock_factor: float


# ============================================================================
# DIVERGENCE RESULT
# ============================================================================

@dataclass(frozen=True)
class DivergenceResult:
    """
    Complete output from the divergence engine.
    """

    symbol: str

    # ------------------------------------------------------------------------
    # FINAL SIGNAL
    # ------------------------------------------------------------------------

    divergence_score: float

    severity: str

    divergence_type: str

    # ------------------------------------------------------------------------
    # PRICE
    # ------------------------------------------------------------------------

    price_return: float

    price_shock_zscore: float

    price_shock_score: float

    intraday_shock_score: float

    final_price_shock_score: float

    # ------------------------------------------------------------------------
    # NEWS
    # ------------------------------------------------------------------------

    net_score: float

    composite_score: float

    n_eff: float

    news_coverage_score: float

    news_coverage_gap: float

    news_alignment_score: float

    polarity: Optional[float]

    directional_density: Optional[float]

    signal_purity: Optional[float]

    # ------------------------------------------------------------------------
    # VOLUME
    # ------------------------------------------------------------------------

    volume_ratio: Optional[float]

    volume_shock_factor: float

    # ------------------------------------------------------------------------
    # DIAGNOSTICS
    # ------------------------------------------------------------------------

    price_direction: str

    news_direction: str

    is_price_shock: bool

    is_news_vacuum: bool

    # ------------------------------------------------------------------------
    # INTERPRETATION
    # ------------------------------------------------------------------------

    interpretation: str

    # ------------------------------------------------------------------------
    # SERIALIZATION
    # ------------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:

        return asdict(self)


# ============================================================================
# DIVERGENCE ENGINE
# ============================================================================

class DivergenceEngine:
    """
    Standalone PRICE SHOCK -> NEWS COVERAGE GAP detection engine.

    The engine has no knowledge of:

        - LMDB
        - article storage
        - article retrieval
        - sentiment model implementation
        - news ingestion
        - NLP

    It operates strictly on completed metrics.
    """

    def __init__(
        self,

        min_effective_volume: float = (
            DEFAULT_MIN_EFFECTIVE_VOLUME
        ),

        price_shock_sigma: float = (
            DEFAULT_PRICE_SHOCK_SIGMA
        ),

        coverage_exponent: float = (
            DEFAULT_COVERAGE_EXPONENT
        ),

        max_volume_factor: float = (
            DEFAULT_MAX_VOLUME_FACTOR
        ),

        news_neutral_threshold: float = (
            DEFAULT_NEWS_NEUTRAL_THRESHOLD
        ),

        watch_threshold: float = (
            DEFAULT_WATCH_THRESHOLD
        ),

        moderate_threshold: float = (
            DEFAULT_MODERATE_THRESHOLD
        ),

        strong_threshold: float = (
            DEFAULT_STRONG_THRESHOLD
        ),

        major_threshold: float = (
            DEFAULT_MAJOR_THRESHOLD
        ),

        extreme_threshold: float = (
            DEFAULT_EXTREME_THRESHOLD
        ),
    ) -> None:

        if min_effective_volume <= 0:
            raise ValueError(
                "min_effective_volume must be > 0"
            )

        if price_shock_sigma <= 0:
            raise ValueError(
                "price_shock_sigma must be > 0"
            )

        if coverage_exponent <= 0:
            raise ValueError(
                "coverage_exponent must be > 0"
            )

        if max_volume_factor < 1.0:
            raise ValueError(
                "max_volume_factor must be >= 1.0"
            )

        self.min_effective_volume = (
            min_effective_volume
        )

        self.price_shock_sigma = (
            price_shock_sigma
        )

        self.coverage_exponent = (
            coverage_exponent
        )

        self.max_volume_factor = (
            max_volume_factor
        )

        self.news_neutral_threshold = (
            news_neutral_threshold
        )

        self.watch_threshold = (
            watch_threshold
        )

        self.moderate_threshold = (
            moderate_threshold
        )

        self.strong_threshold = (
            strong_threshold
        )

        self.major_threshold = (
            major_threshold
        )

        self.extreme_threshold = (
            extreme_threshold
        )

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def analyze(
        self,
        symbol: str,
        news: NewsMetrics,
        market: MarketMetrics,
    ) -> DivergenceResult:
        """
        Execute complete divergence analysis.

        Parameters
        ----------
        symbol:
            Stock ticker.

        news:
            Completed sentiment metrics.

        market:
            Current and historical market metrics.

        Returns
        -------
        DivergenceResult
        """

        symbol = symbol.upper().strip()

        self._validate_market(market)

        self._validate_news(news)

        # --------------------------------------------------------------------
        # PRICE SHOCK
        # --------------------------------------------------------------------

        price_metrics = (
            self._calculate_price_shock_metrics(
                market
            )
        )

        # --------------------------------------------------------------------
        # NEWS COVERAGE
        # --------------------------------------------------------------------

        news_coverage_score = (
            self._calculate_news_coverage_score(
                news.n_eff
            )
        )

        news_coverage_gap = (
            1.0
            - news_coverage_score
        )

        # --------------------------------------------------------------------
        # NEWS ALIGNMENT
        # --------------------------------------------------------------------

        news_alignment_score = (
            self._calculate_news_alignment(
                news
            )
        )

        # --------------------------------------------------------------------
        # VOLUME
        # --------------------------------------------------------------------

        volume_metrics = (
            self._calculate_volume_metrics(
                market
            )
        )

        # --------------------------------------------------------------------
        # FINAL DIVERGENCE SCORE
        # --------------------------------------------------------------------

        divergence_score = (
            self._calculate_divergence_score(
                price_shock_score=(
                    price_metrics.final_price_shock_score
                ),

                news_coverage_gap=(
                    news_coverage_gap
                ),

                news_alignment_score=(
                    news_alignment_score
                ),

                volume_shock_factor=(
                    volume_metrics.volume_shock_factor
                ),
            )
        )

        # --------------------------------------------------------------------
        # FLAGS
        # --------------------------------------------------------------------

        is_price_shock = (
            price_metrics.final_price_shock_score
            >= 0.50
        )

        is_news_vacuum = (
            news_coverage_gap
            >= 0.75
        )

        # --------------------------------------------------------------------
        # DIRECTIONS
        # --------------------------------------------------------------------

        price_direction = (
            self._get_price_direction(
                price_metrics.daily_return
            )
        )

        news_direction = (
            self._get_news_direction(
                news.composite_score
            )
        )

        # --------------------------------------------------------------------
        # TYPE
        # --------------------------------------------------------------------

        divergence_type = (
            self._classify_divergence_type(
                price_return=(
                    price_metrics.daily_return
                ),

                composite_score=(
                    news.composite_score
                ),

                divergence_score=(
                    divergence_score
                ),
            )
        )

        # --------------------------------------------------------------------
        # SEVERITY
        # --------------------------------------------------------------------

        severity = (
            self._classify_severity(
                divergence_score
            )
        )

        # --------------------------------------------------------------------
        # INTERPRETATION
        # --------------------------------------------------------------------

        interpretation = (
            self._build_interpretation(
                symbol=symbol,
                price_metrics=price_metrics,
                news=news,
                news_coverage_gap=(
                    news_coverage_gap
                ),
                divergence_score=(
                    divergence_score
                ),
                severity=severity,
                divergence_type=(
                    divergence_type
                ),
                volume_metrics=(
                    volume_metrics
                ),
            )
        )

        return DivergenceResult(

            symbol=symbol,

            divergence_score=(
                divergence_score
            ),

            severity=(
                severity.value
            ),

            divergence_type=(
                divergence_type.value
            ),

            # PRICE
            price_return=(
                price_metrics.daily_return
            ),

            price_shock_zscore=(
                price_metrics.price_shock_zscore
            ),

            price_shock_score=(
                price_metrics.price_shock_score
            ),

            intraday_shock_score=(
                price_metrics.intraday_shock_score
            ),

            final_price_shock_score=(
                price_metrics.final_price_shock_score
            ),

            # NEWS
            net_score=news.net_score,

            composite_score=news.composite_score,

            n_eff=news.n_eff,

            news_coverage_score=(
                news_coverage_score
            ),

            news_coverage_gap=(
                news_coverage_gap
            ),

            news_alignment_score=(
                news_alignment_score
            ),

            polarity=news.polarity,

            directional_density=(
                news.directional_density
            ),

            signal_purity=news.signal_purity,

            # VOLUME
            volume_ratio=(
                volume_metrics.volume_ratio
            ),

            volume_shock_factor=(
                volume_metrics.volume_shock_factor
            ),

            # DIAGNOSTICS
            price_direction=(
                price_direction
            ),

            news_direction=(
                news_direction
            ),

            is_price_shock=(
                is_price_shock
            ),

            is_news_vacuum=(
                is_news_vacuum
            ),

            # INTERPRETATION
            interpretation=(
                interpretation
            ),
        )

    # ========================================================================
    # PRICE SHOCK
    # ========================================================================

    def _calculate_price_shock_metrics(
        self,
        market: MarketMetrics,
    ) -> PriceShockMetrics:
        """
        Calculate daily and intraday price shock.

        Daily:

            z = return / historical_std

            score = min(1, abs(z) / 3)

        Intraday:

            Use the largest available intraday return.

        Final:

            max(daily_score, intraday_score)
        """

        daily_return = (
            market.current_price
            - market.previous_close
        ) / market.previous_close

        returns = (
            self._clean_numeric_list(
                market.historical_returns
            )
        )

        if len(returns) < 2:

            raise ValueError(
                "At least two historical returns "
                "are required."
            )

        return_std = (
            statistics.stdev(
                returns
            )
        )

        if return_std <= 0:

            zscore = 0.0

            daily_score = 0.0

        else:

            zscore = (
                daily_return
                / return_std
            )

            daily_score = min(
                1.0,
                abs(zscore)
                / self.price_shock_sigma,
            )

        # --------------------------------------------------------------------
        # INTRADAY
        # --------------------------------------------------------------------

        intraday_returns = []

        if market.return_15m is not None:
            intraday_returns.append(
                abs(market.return_15m)
            )

        if market.return_1h is not None:
            intraday_returns.append(
                abs(market.return_1h)
            )

        if market.return_4h is not None:
            intraday_returns.append(
                abs(market.return_4h)
            )

        if intraday_returns:

            max_intraday_return = max(
                intraday_returns
            )

            intraday_score = min(
                1.0,
                max_intraday_return
                / (
                    self.price_shock_sigma
                    * return_std
                ),
            )

        else:

            intraday_score = 0.0

        final_score = max(
            daily_score,
            intraday_score,
        )

        return PriceShockMetrics(

            daily_return=daily_return,

            return_std=return_std,

            price_shock_zscore=zscore,

            price_shock_score=daily_score,

            intraday_shock_score=(
                intraday_score
            ),

            final_price_shock_score=(
                final_score
            ),

            direction=(
                self._get_price_direction(
                    daily_return
                )
            ),
        )

    # ========================================================================
    # NEWS COVERAGE
    # ========================================================================

    def _calculate_news_coverage_score(
        self,
        n_eff: float,
    ) -> float:
        """
        Calculate normalized fresh news coverage.

            NCS = min(1, n_eff / target)

        target defaults to 3 effective articles.
        """

        return min(
            1.0,
            n_eff
            / self.min_effective_volume,
        )

    # ========================================================================
    # NEWS ALIGNMENT
    # ========================================================================

    @staticmethod
    def _calculate_news_alignment(
        news: NewsMetrics,
    ) -> float:
        """
        Measure strength of current/fresh news direction.

        We intentionally use composite_score rather than net_score.

        Why?

            net_score
                = broader sentiment direction

            composite_score
                = freshness-weighted current signal

        Divergence is specifically asking whether current market
        behavior is explained by current information.

        Therefore:

            NAS = abs(composite_score)
        """

        return min(
            1.0,
            abs(news.composite_score),
        )

    # ========================================================================
    # VOLUME
    # ========================================================================

    def _calculate_volume_metrics(
        self,
        market: MarketMetrics,
    ) -> VolumeShockMetrics:
        """
        Calculate volume shock.

            volume_ratio =
                current_volume / average_volume

            VSF =
                min(1.5, sqrt(volume_ratio))
        """

        if (
            market.current_volume is None
            or not market.historical_volumes
        ):

            return VolumeShockMetrics(
                current_volume=(
                    market.current_volume
                ),

                average_volume=None,

                volume_ratio=None,

                volume_shock_factor=1.0,
            )

        volumes = (
            self._clean_numeric_list(
                market.historical_volumes
            )
        )

        if not volumes:

            return VolumeShockMetrics(
                current_volume=(
                    market.current_volume
                ),

                average_volume=None,

                volume_ratio=None,

                volume_shock_factor=1.0,
            )

        average_volume = (
            statistics.mean(
                volumes
            )
        )

        if average_volume <= 0:

            return VolumeShockMetrics(
                current_volume=(
                    market.current_volume
                ),

                average_volume=(
                    average_volume
                ),

                volume_ratio=None,

                volume_shock_factor=1.0,
            )

        volume_ratio = (
            market.current_volume
            / average_volume
        )

        volume_shock_factor = min(
            self.max_volume_factor,
            math.sqrt(
                max(
                    volume_ratio,
                    0.0,
                )
            ),
        )

        return VolumeShockMetrics(

            current_volume=(
                market.current_volume
            ),

            average_volume=(
                average_volume
            ),

            volume_ratio=(
                volume_ratio
            ),

            volume_shock_factor=(
                volume_shock_factor
            ),
        )

    # ========================================================================
    # FINAL DIVERGENCE SCORE
    # ========================================================================

    def _calculate_divergence_score(
        self,
        price_shock_score: float,
        news_coverage_gap: float,
        news_alignment_score: float,
        volume_shock_factor: float,
    ) -> float:
        """
        Final divergence formula.

            DAS =
                PSS
                * NCG^1.5
                * (1 - NAS)
                * VSF

        Where:

            PSS
                Price Shock Score

            NCG
                News Coverage Gap

            NAS
                News Alignment Score

            VSF
                Volume Shock Factor
        """

        score = (
            price_shock_score
            * (
                news_coverage_gap
                ** self.coverage_exponent
            )
            * (
                1.0
                - news_alignment_score
            )
            * volume_shock_factor
        )

        return min(
            1.0,
            max(
                0.0,
                score,
            ),
        )

    # ========================================================================
    # DIVERGENCE TYPE
    # ========================================================================

    def _classify_divergence_type(
        self,
        price_return: float,
        composite_score: float,
        divergence_score: float,
    ) -> DivergenceType:
        """
        Determine the qualitative relationship between price
        and fresh news.
        """

        if (
            divergence_score
            < self.watch_threshold
        ):
            return DivergenceType.NONE

        price_bullish = (
            price_return > 0
        )

        price_bearish = (
            price_return < 0
        )

        news_bullish = (
            composite_score
            >= self.news_neutral_threshold
        )

        news_bearish = (
            composite_score
            <= -self.news_neutral_threshold
        )

        news_neutral = not (
            news_bullish
            or news_bearish
        )

        # --------------------------------------------------------------------
        # BULLISH PRICE
        # --------------------------------------------------------------------

        if price_bullish:

            if news_neutral:

                return (
                    DivergenceType
                    .BULLISH_PRICE_NEWS_VACUUM
                )

            if news_bearish:

                return (
                    DivergenceType
                    .BULLISH_PRICE_BEARISH_NEWS
                )

            return (
                DivergenceType
                .BULLISH_CONFIRMATION
            )

        # --------------------------------------------------------------------
        # BEARISH PRICE
        # --------------------------------------------------------------------

        if price_bearish:

            if news_neutral:

                return (
                    DivergenceType
                    .BEARISH_PRICE_NEWS_VACUUM
                )

            if news_bullish:

                return (
                    DivergenceType
                    .BEARISH_PRICE_BULLISH_NEWS
                )

            return (
                DivergenceType
                .BEARISH_CONFIRMATION
            )

        return DivergenceType.NONE

    # ========================================================================
    # SEVERITY
    # ========================================================================

    def _classify_severity(
        self,
        score: float,
    ) -> DivergenceSeverity:

        if score >= self.extreme_threshold:

            return DivergenceSeverity.EXTREME

        if score >= self.major_threshold:

            return DivergenceSeverity.MAJOR

        if score >= self.strong_threshold:

            return DivergenceSeverity.STRONG

        if score >= self.moderate_threshold:

            return DivergenceSeverity.MODERATE

        if score >= self.watch_threshold:

            return DivergenceSeverity.WATCH

        return DivergenceSeverity.NONE

    # ========================================================================
    # DIRECTION
    # ========================================================================

    @staticmethod
    def _get_price_direction(
        return_value: float,
    ) -> str:

        if return_value > 0:

            return "Bullish"

        if return_value < 0:

            return "Bearish"

        return "Neutral"

    # ========================================================================

    def _get_news_direction(
        self,
        composite_score: float,
    ) -> str:

        if (
            composite_score
            >= self.news_neutral_threshold
        ):

            return "Bullish"

        if (
            composite_score
            <= -self.news_neutral_threshold
        ):

            return "Bearish"

        return "Neutral"

    # ========================================================================
    # INTERPRETATION
    # ========================================================================

    def _build_interpretation(
        self,
        symbol: str,
        price_metrics: PriceShockMetrics,
        news: NewsMetrics,
        news_coverage_gap: float,
        divergence_score: float,
        severity: DivergenceSeverity,
        divergence_type: DivergenceType,
        volume_metrics: VolumeShockMetrics,
    ) -> str:
        """
        Build a concise machine-generated explanation.

        This is intended for UI/logging/alerts, not for the
        mathematical calculation itself.
        """

        volume_text = ""

        if volume_metrics.volume_ratio is not None:

            volume_text = (
                f" Volume is "
                f"{volume_metrics.volume_ratio:.2f}x "
                f"the historical average."
            )

        return (
            f"{symbol}: "
            f"{severity.value}. "
            f"{divergence_type.value}. "

            f"Price moved "
            f"{price_metrics.daily_return:+.2%} "
            f"with a price shock z-score of "
            f"{price_metrics.price_shock_zscore:+.2f} "
            f"and normalized shock score "
            f"{price_metrics.final_price_shock_score:.3f}. "

            f"Fresh effective news volume is "
            f"{news.n_eff:.2f}, producing a "
            f"{news_coverage_gap:.1%} news coverage gap. "

            f"Fresh composite sentiment is "
            f"{news.composite_score:+.4f}, "
            f"while broader net sentiment is "
            f"{news.net_score:+.4f}. "

            f"Divergence score is "
            f"{divergence_score:.3f}."

            f"{volume_text}"
        )

    # ========================================================================
    # VALIDATION
    # ========================================================================

    @staticmethod
    def _validate_market(
        market: MarketMetrics,
    ) -> None:

        if market.current_price <= 0:

            raise ValueError(
                "current_price must be > 0"
            )

        if market.previous_close <= 0:

            raise ValueError(
                "previous_close must be > 0"
            )

        if len(
            market.historical_returns
        ) < 2:

            raise ValueError(
                "historical_returns must contain "
                "at least two observations."
            )

        if (
            market.current_volume is not None
            and market.current_volume < 0
        ):

            raise ValueError(
                "current_volume cannot be negative."
            )

    # ========================================================================

    @staticmethod
    def _validate_news(
        news: NewsMetrics,
    ) -> None:

        if news.n_eff < 0:

            raise ValueError(
                "n_eff cannot be negative."
            )

        for name, value in (
            ("net_score", news.net_score),
            ("composite_score", news.composite_score),
        ):

            if not math.isfinite(value):

                raise ValueError(
                    f"{name} must be finite."
                )

    # ========================================================================
    # UTILITIES
    # ========================================================================

    @staticmethod
    def _clean_numeric_list(
        values: Iterable[float],
    ) -> list[float]:

        cleaned: list[float] = []

        for value in values:

            if value is None:
                continue

            try:

                numeric_value = float(
                    value
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            if not math.isfinite(
                numeric_value
            ):

                continue

            cleaned.append(
                numeric_value
            )

        return cleaned


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

def calculate_divergence(
    symbol: str,
    news: NewsMetrics,
    market: MarketMetrics,
) -> DivergenceResult:
    """
    Convenience function for callers that don't need to
    instantiate and maintain a DivergenceEngine.
    """

    engine = DivergenceEngine()

    return engine.analyze(
        symbol=symbol,
        news=news,
        market=market,
    )