from __future__ import annotations

import math
import statistics
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping

if TYPE_CHECKING:
    from composite_score import CompositeScorer


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
DEFAULT_DB_ID = "0001"
DEFAULT_LMDB_PATH = Path("datastore") / "LMDB_0001"

# ################################# Class ENUM 1
# Core Class ENUM
# Named constant values for Type Safety and Validation
class DivergenceSeverity(str, Enum):
    NONE = "None"
    WATCH = "Watch"
    MODERATE = "Moderate Divergence"
    STRONG = "Strong Divergence"
    MAJOR = "Major Divergence"
    EXTREME = "Extreme Divergence"

# ################################# Class ENUM 2
# Core Class ENUM
# Named constant values for Type Safety and Validation
class DivergenceType(str, Enum):
    NONE = "None"
    BULLISH_PRICE_NEWS_VACUUM = "Bullish Price + News Vacuum"
    BULLISH_PRICE_BEARISH_NEWS = "Bullish Price + Bearish News"
    BEARISH_PRICE_NEWS_VACUUM = "Bearish Price + News Vacuum"
    BEARISH_PRICE_BULLISH_NEWS = "Bearish Price + Bullish News"
    BULLISH_CONFIRMATION = "Bullish Price + Bullish News Confirmation"
    BEARISH_CONFIRMATION = "Bearish Price + Bearish News Confirmation"


# ################################# Core Class Decorated Method
# Immutable Data structure: NewsMetrics
# Helper Method 1: from_composite_report
# Helper Method 2: from_mapping
# Helper Method 3: to_dict
@dataclass(frozen=True)
class NewsMetrics:
    """News metrics produced by composite_score.py."""

    composite_score: float
    n_eff: float
    net_score: float = 0.0
    polarity: float | None = None
    directional_density: float | None = None
    signal_purity: float | None = None
    state: str | None = None

    @classmethod
    def from_composite_report(
        cls,
        report: Mapping[str, Any],
        legacy_report: Mapping[str, Any] | None = None,
        polarity_report: Mapping[str, Any] | None = None,
    ) -> "NewsMetrics":
        composite = _float_or_zero(report.get("composite_score"))
        legacy = legacy_report or {}
        polarity_source = polarity_report or report

        return cls(
            composite_score=composite,
            n_eff=_float_or_zero(report.get("n_eff")),
            net_score=_float_or_zero(legacy.get("net_score", report.get("net_score", composite))),
            polarity=_optional_float(polarity_source.get("polarity")),
            directional_density=_optional_float(report.get("directional_density")),
            signal_purity=_optional_float(
                legacy.get("signal_purity", report.get("signal_purity"))
            ),
            state=_optional_text(report.get("state")),
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "NewsMetrics":
        if "composite_score" in data and "n_eff" in data:
            return cls.from_composite_report(data)

        return cls(
            composite_score=_float_or_zero(data.get("composite_score")),
            n_eff=_float_or_zero(data.get("n_eff")),
            net_score=_float_or_zero(data.get("net_score")),
            polarity=_optional_float(data.get("polarity")),
            directional_density=_optional_float(data.get("directional_density")),
            signal_purity=_optional_float(data.get("signal_purity")),
            state=_optional_text(data.get("state")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ################################# Core Class Decorated Method
# Immutable Data structure: MarketMetrics
# Helper Method: from_mapping
@dataclass(frozen=True)
class MarketMetrics:
    current_price: float
    previous_close: float
    historical_returns: list[float]
    current_volume: float | None = None
    historical_volumes: list[float] | None = None
    return_15m: float | None = None
    return_1h: float | None = None
    return_4h: float | None = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "MarketMetrics":
        returns = data.get("historical_returns")
        if returns is None:
            returns = _returns_from_closes(data.get("historical_closes", []))

        return cls(
            current_price=float(data["current_price"]),
            previous_close=float(data["previous_close"]),
            historical_returns=[float(x) for x in returns],
            current_volume=_optional_float(
                data.get("current_volume", data.get("today_volume"))
            ),
            historical_volumes=_optional_float_list(data.get("historical_volumes")),
            return_15m=_optional_float(data.get("return_15m")),
            return_1h=_optional_float(data.get("return_1h")),
            return_4h=_optional_float(data.get("return_4h")),
        )

# ################################# Core Class Named Data structure
# Immutable Data structure: PriceShockMetrics
@dataclass(frozen=True)
class PriceShockMetrics:
    daily_return: float
    return_std: float
    price_shock_zscore: float
    price_shock_score: float
    intraday_shock_score: float
    final_price_shock_score: float
    direction: str


# ################################# Core Class Named Data structure
# Immutable Data structure: VolumeShockMetrics
@dataclass(frozen=True)
class VolumeShockMetrics:
    current_volume: float | None
    average_volume: float | None
    volume_ratio: float | None
    volume_shock_factor: float


# ################################# Core Class Named Data structure
# named Data structure: DivergenceResult
# Immutable (read-only)
# Converter method (converts to dict{})
@dataclass(frozen=True)
class DivergenceResult:
    symbol: str
    divergence_score: float
    severity: str
    divergence_type: str
    price_return: float
    price_shock_zscore: float
    price_shock_score: float
    intraday_shock_score: float
    final_price_shock_score: float
    net_score: float
    composite_score: float
    n_eff: float
    news_coverage_score: float
    news_coverage_gap: float
    news_alignment_score: float
    polarity: float | None
    directional_density: float | None
    signal_purity: float | None
    volume_ratio: float | None
    volume_shock_factor: float
    price_direction: str
    news_direction: str
    is_price_shock: bool
    is_news_vacuum: bool
    interpretation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ################################# Core Main Class
# Core Divergecne engine controller
#
class DivergenceEngine:
    """Detect price/news divergence using CompositeScorer news metrics."""

    def __init__(
        self,
        *,
        scorer: CompositeScorer | None = None,
        min_effective_volume: float = DEFAULT_MIN_EFFECTIVE_VOLUME,
        price_shock_sigma: float = DEFAULT_PRICE_SHOCK_SIGMA,
        coverage_exponent: float = DEFAULT_COVERAGE_EXPONENT,
        max_volume_factor: float = DEFAULT_MAX_VOLUME_FACTOR,
        news_neutral_threshold: float = DEFAULT_NEWS_NEUTRAL_THRESHOLD,
        watch_threshold: float = DEFAULT_WATCH_THRESHOLD,
        moderate_threshold: float = DEFAULT_MODERATE_THRESHOLD,
        strong_threshold: float = DEFAULT_STRONG_THRESHOLD,
        major_threshold: float = DEFAULT_MAJOR_THRESHOLD,
        extreme_threshold: float = DEFAULT_EXTREME_THRESHOLD,
    ) -> None:
        if min_effective_volume <= 0:
            raise ValueError("min_effective_volume must be > 0")
        if price_shock_sigma <= 0:
            raise ValueError("price_shock_sigma must be > 0")
        if coverage_exponent <= 0:
            raise ValueError("coverage_exponent must be > 0")
        if max_volume_factor < 1.0:
            raise ValueError("max_volume_factor must be >= 1.0")

        self.scorer = scorer
        self.min_effective_volume = min_effective_volume
        self.price_shock_sigma = price_shock_sigma
        self.coverage_exponent = coverage_exponent
        self.max_volume_factor = max_volume_factor
        self.news_neutral_threshold = news_neutral_threshold
        self.watch_threshold = watch_threshold
        self.moderate_threshold = moderate_threshold
        self.strong_threshold = strong_threshold
        self.major_threshold = major_threshold
        self.extreme_threshold = extreme_threshold

    # ######################### Core Main Class Method 1
    #
    def analyze(
        self,
        symbol: str,
        news: NewsMetrics | Mapping[str, Any],
        market: MarketMetrics | Mapping[str, Any],
    ) -> DivergenceResult:
        news_metrics = self._coerce_news(news)
        market_metrics = self._coerce_market(market)
        symbol = symbol.upper().strip()

        self._validate_news(news_metrics)
        self._validate_market(market_metrics)

        price = self._calculate_price_shock_metrics(market_metrics)
        volume = self._calculate_volume_metrics(market_metrics)
        coverage_score = min(1.0, news_metrics.n_eff / self.min_effective_volume)
        coverage_gap = 1.0 - coverage_score
        alignment_score = min(1.0, abs(news_metrics.composite_score))

        divergence_score = self._calculate_divergence_score(
            price.final_price_shock_score,
            coverage_gap,
            alignment_score,
            volume.volume_shock_factor,
        )
        severity = self._classify_severity(divergence_score)
        divergence_type = self._classify_divergence_type(
            price.daily_return,
            news_metrics.composite_score,
            divergence_score,
        )
        news_direction = self._get_news_direction(news_metrics.composite_score)

        return DivergenceResult(
            symbol=symbol,
            divergence_score=divergence_score,
            severity=severity.value,
            divergence_type=divergence_type.value,
            price_return=price.daily_return,
            price_shock_zscore=price.price_shock_zscore,
            price_shock_score=price.price_shock_score,
            intraday_shock_score=price.intraday_shock_score,
            final_price_shock_score=price.final_price_shock_score,
            net_score=news_metrics.net_score,
            composite_score=news_metrics.composite_score,
            n_eff=news_metrics.n_eff,
            news_coverage_score=coverage_score,
            news_coverage_gap=coverage_gap,
            news_alignment_score=alignment_score,
            polarity=news_metrics.polarity,
            directional_density=news_metrics.directional_density,
            signal_purity=news_metrics.signal_purity,
            volume_ratio=volume.volume_ratio,
            volume_shock_factor=volume.volume_shock_factor,
            price_direction=price.direction,
            news_direction=news_direction,
            is_price_shock=price.final_price_shock_score >= 0.50,
            is_news_vacuum=coverage_gap >= 0.75,
            interpretation=self._build_interpretation(
                symbol,
                price,
                news_metrics,
                coverage_gap,
                divergence_score,
                severity,
                divergence_type,
                volume,
            ),
        )

    # ######################### Core Main Class Method 2
    #
    def analyze_from_composite(
        self,
        symbol: str,
        composite_report: Mapping[str, Any],
        market: MarketMetrics | Mapping[str, Any],
        legacy_report: Mapping[str, Any] | None = None,
        polarity_report: Mapping[str, Any] | None = None,
    ) -> DivergenceResult:
        news = NewsMetrics.from_composite_report(
            composite_report,
            legacy_report=legacy_report,
            polarity_report=polarity_report,
        )
        return self.analyze(symbol, news, market)

    # ######################### Core Main Class Method 3
    #
    def analyze_articles(
        self,
        symbol: str,
        articles: Iterable[Mapping[str, Any]],
        market: MarketMetrics | Mapping[str, Any],
        run_epoch: float | None = None,
    ) -> DivergenceResult:
        report = self._get_scorer().score_symbol(symbol, articles, run_epoch or time.time())
        return self.analyze_from_composite(symbol, report, market)

    # ######################### Core Main Class Method 4
    #
    def analyze_lmdb(
        self,
        symbol: str,
        market: MarketMetrics | Mapping[str, Any],
        db_path: str = str(DEFAULT_LMDB_PATH),
        run_epoch: float | None = None,
        db_id: str = DEFAULT_DB_ID,
    ) -> DivergenceResult:
        report = self._get_scorer().score_symbol_from_lmdb(
            symbol,
            db_path=db_path,
            run_epoch=run_epoch or time.time(),
            db_id=db_id,
        )
        return self.analyze_from_composite(symbol, report, market)

    # ######################### Core Main Class Method 5
    #
    def _calculate_price_shock_metrics(self, market: MarketMetrics) -> PriceShockMetrics:
        daily_return = (market.current_price - market.previous_close) / market.previous_close
        returns = _clean_numeric_list(market.historical_returns)
        return_std = statistics.stdev(returns)

        if return_std <= 0:
            zscore = daily_score = intraday_score = 0.0
        else:
            zscore = daily_return / return_std
            daily_score = min(1.0, abs(zscore) / self.price_shock_sigma)
            intraday_return = max(
                [0.0]
                + [
                    abs(x)
                    for x in (market.return_15m, market.return_1h, market.return_4h)
                    if x is not None
                ]
            )
            intraday_score = min(
                1.0,
                intraday_return / (self.price_shock_sigma * return_std),
            )

        return PriceShockMetrics(
            daily_return=daily_return,
            return_std=return_std,
            price_shock_zscore=zscore,
            price_shock_score=daily_score,
            intraday_shock_score=intraday_score,
            final_price_shock_score=max(daily_score, intraday_score),
            direction=self._get_price_direction(daily_return),
        )
    # ######################### Core Main Class Method 6
    #
    def _calculate_volume_metrics(self, market: MarketMetrics) -> VolumeShockMetrics:
        volumes = _clean_numeric_list(market.historical_volumes or [])
        if market.current_volume is None or not volumes:
            return VolumeShockMetrics(market.current_volume, None, None, 1.0)

        average_volume = statistics.mean(volumes)
        if average_volume <= 0:
            return VolumeShockMetrics(market.current_volume, average_volume, None, 1.0)

        ratio = market.current_volume / average_volume
        factor = min(self.max_volume_factor, math.sqrt(max(ratio, 0.0)))
        return VolumeShockMetrics(market.current_volume, average_volume, ratio, factor)


    # ##################################
    # Calculate the Divergence Score
    # - Quant Statitsical formula used...
    # score => 
    # price_shock_score x (news_coverage_gap ^ coverage_exponent)  x (1.0 - news_alignment_score) x volume_shock_factor

    # ######################### Core Main Class Helper Method 1
    #
    def _calculate_divergence_score(
        self,
        price_shock_score: float,
        news_coverage_gap: float,
        news_alignment_score: float,
        volume_shock_factor: float,
    ) -> float:
        score = (
            price_shock_score
            * (news_coverage_gap ** self.coverage_exponent)
            * (1.0 - news_alignment_score)
            * volume_shock_factor
        )
        return min(1.0, max(0.0, score))

    # ######################### Core Main Class Method 2
    # Provide a Written description of the Divergence score
    #    
    def _classify_divergence_type(
        self,
        price_return: float,
        composite_score: float,
        divergence_score: float,
    ) -> DivergenceType:
        if divergence_score < self.watch_threshold or price_return == 0:
            return DivergenceType.NONE

        price_bullish = price_return > 0
        news_bullish = composite_score >= self.news_neutral_threshold
        news_bearish = composite_score <= -self.news_neutral_threshold

        if price_bullish and news_bearish:
            return DivergenceType.BULLISH_PRICE_BEARISH_NEWS
        if price_bullish and news_bullish:
            return DivergenceType.BULLISH_CONFIRMATION
        if price_bullish:
            return DivergenceType.BULLISH_PRICE_NEWS_VACUUM
        if news_bullish:
            return DivergenceType.BEARISH_PRICE_BULLISH_NEWS
        if news_bearish:
            return DivergenceType.BEARISH_CONFIRMATION
        return DivergenceType.BEARISH_PRICE_NEWS_VACUUM

    # ######################### Core Main Class Method 3
    # provide a written description of the Divergecne Serverity
    #
    def _classify_severity(self, score: float) -> DivergenceSeverity:
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

    # ######################### Core Main Class Decorated Helper Methods
    #  
    @staticmethod
    def _get_price_direction(return_value: float) -> str:
        if return_value > 0:
            return "Bullish"
        if return_value < 0:
            return "Bearish"
        return "Neutral"

    def _get_news_direction(self, composite_score: float) -> str:
        if composite_score >= self.news_neutral_threshold:
            return "Bullish"
        if composite_score <= -self.news_neutral_threshold:
            return "Bearish"
        return "Neutral"

    def _get_scorer(self) -> CompositeScorer:
        if self.scorer is None:
            from composite_score import CompositeScorer

            self.scorer = CompositeScorer()
        return self.scorer

    @staticmethod
    def _build_interpretation(
        symbol: str,
        price: PriceShockMetrics,
        news: NewsMetrics,
        coverage_gap: float,
        score: float,
        severity: DivergenceSeverity,
        divergence_type: DivergenceType,
        volume: VolumeShockMetrics,
    ) -> str:
        volume_text = (
            f"// Volume is: {volume.volume_ratio:.2f} X Historical average !"
            if volume.volume_ratio is not None
            else ""
        )
        # TODO: Buld a pure dict{} of the interpretation and return that instead
        # - composite score can print it's report by passing tghe dict
        # - and supabase engine can just embed thenative  returned dict in its payload
        # - that was the WebUX has a pure dict to work with and not a long messy text string

        return (
            f"Stock symbol:  {symbol:6} // Severity: {severity.value} // Divergence alert: {divergence_type.value}\n"
            f"Price moved:  {price.daily_return:+7.2%} // News/Price movement shock score: {price.final_price_shock_score:.3f}\n"
            f"Composite news score:     {news.composite_score:+.4f} with Freshness (n_eff): {news.n_eff:.2f}\n"
            f"News coverage vaccum gap: {coverage_gap:.1%} \nOverall Divergence score alert: {score:.3f} {volume_text}"
        )

    @staticmethod
    def _coerce_news(news: NewsMetrics | Mapping[str, Any]) -> NewsMetrics:
        return news if isinstance(news, NewsMetrics) else NewsMetrics.from_mapping(news)

    @staticmethod
    def _coerce_market(market: MarketMetrics | Mapping[str, Any]) -> MarketMetrics:
        return market if isinstance(market, MarketMetrics) else MarketMetrics.from_mapping(market)

    @staticmethod
    def _validate_market(market: MarketMetrics) -> None:
        if market.current_price <= 0:
            raise ValueError("current_price must be > 0")
        if market.previous_close <= 0:
            raise ValueError("previous_close must be > 0")
        if len(_clean_numeric_list(market.historical_returns)) < 2:
            raise ValueError("historical_returns must contain at least two observations")
        if market.current_volume is not None and market.current_volume < 0:
            raise ValueError("current_volume cannot be negative")

    @staticmethod
    def _validate_news(news: NewsMetrics) -> None:
        if news.n_eff < 0:
            raise ValueError("n_eff cannot be negative")
        for name, value in (
            ("net_score", news.net_score),
            ("composite_score", news.composite_score),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")

# ######################### Core Method
#  

def calculate_divergence(
    symbol: str,
    news: NewsMetrics | Mapping[str, Any],
    market: MarketMetrics | Mapping[str, Any],
) -> DivergenceResult:
    return DivergenceEngine().analyze(symbol, news, market)


# ######################### Core Heklper Method 2
def _clean_numeric_list(values: Iterable[Any]) -> list[float]:
    if values is None or isinstance(values, (str, bytes)):
        return []
    try:
        iterator = iter(values)
    except TypeError:
        return []

    cleaned: list[float] = []
    for value in iterator:
        parsed = _optional_float(value)
        if parsed is not None and math.isfinite(parsed):
            cleaned.append(parsed)
    return cleaned

# ######################### Core Method 2
def _returns_from_closes(closes: Iterable[Any]) -> list[float]:
    prices = [x for x in _clean_numeric_list(closes) if x > 0]
    return [
        (current - previous) / previous
        for previous, current in zip(prices, prices[1:])
        if previous > 0
    ]

# ######################### Core Method 3
def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


# ######################### Core Method 4
def _optional_float_list(values: Any) -> list[float] | None:
    if values is None or isinstance(values, (str, bytes)):
        return None
    try:
        return _clean_numeric_list(values)
    except TypeError:
        return None

# ######################### Core Method 5
def _float_or_zero(value: Any) -> float:
    return _optional_float(value) or 0.0

# ######################### Core Method 6
def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
