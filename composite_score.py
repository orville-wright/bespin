#!/usr/bin/env python3

"""
Composite sentiment scorer for Bespin news sentiment records.

The scoring math is intentionally kept close to the reviewed template.
This module adds the adapter code needed to collect live Bespin article
records from dicts, lists, pandas DataFrames, and the LMDB article cache.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import lmdb
except ImportError:  # pragma: no cover - dependency is optional until LMDB reads are used.
    lmdb = None


# =====================================================================
# Composite score parameters - FROZEN SHARED CONSTANTS
#
# These MUST be identical for every symbol in a ranking run, and
# identical between both Bespin users, or cross-symbol / cross-user
# comparisons are meaningless. Treat as methodology constants, not
# per-user tunables. If a value is ever changed, historical composite
# scores are no longer comparable to new ones - log the change date.
# =====================================================================

HALF_LIFE_HOURS = 42.0
VOLUME_SHRINKAGE_K = 5.0
DENSITY_EXPONENT = 0.5
MIN_EFFECTIVE_VOLUME = 3.0
SECONDS_PER_HOUR = 3600.0

DEFAULT_DB_ID = "0001"
DEFAULT_LMDB_PATH = Path("datastore") / "LMDB_0001"


class CompositeScorer:
    """
    Compute a single composite sentiment score for one stock ticker.

    Public entry points:
      - composite_score(symbol, articles, run_epoch)
      - score_symbol(symbol, source, run_epoch=None)
      - score_symbol_from_lmdb(symbol, db_path=DEFAULT_LMDB_PATH, ...)
    """

    def __init__(
            self,
            half_life_hours: float = HALF_LIFE_HOURS,
            volume_shrinkage_k: float = VOLUME_SHRINKAGE_K,
            density_exponent: float = DENSITY_EXPONENT,
            min_effective_volume: float = MIN_EFFECTIVE_VOLUME):
        self.half_life_hours = half_life_hours
        self.volume_shrinkage_k = volume_shrinkage_k
        self.density_exponent = density_exponent
        self.min_effective_volume = min_effective_volume
        self.composite_report: dict[str, Any] | None = None

    def score_symbol(
            self,
            symbol: str,
            source: Any,
            run_epoch: float | None = None) -> dict[str, Any]:
        """
        Score one ticker from live Bespin in-memory data.

        `source` may be:
          - one article dict
          - iterable/list of article dicts
          - pandas DataFrame with article or chunk rows

        The adapter accepts both the template fields
        (`published_epoch`, `positive_strength`, etc.) and Bespin LMDB-style
        fields (`iso_age`, `positive_count`, chunk dicts with `sent_type`).
        """
        if run_epoch is None:
            run_epoch = time.time()

        articles = list(self.iter_scoreable_articles(source))
        return self.composite_score(symbol.upper(), articles, run_epoch)

    def score_symbol_from_lmdb(
            self,
            symbol: str,
            db_path: str | Path = DEFAULT_LMDB_PATH,
            run_epoch: float | None = None,
            db_id: str = DEFAULT_DB_ID) -> dict[str, Any]:
        """
        Read all cached LMDB article records for one ticker and score them.

        Current Bespin LMDB keys are shaped as:
            0001.<SYMBOL>.<urlhash>

        Values are JSON article packages produced by ml_yf_nlp_news_engine.
        """
        if run_epoch is None:
            run_epoch = time.time()

        records = list(self.load_symbol_articles_from_lmdb(symbol, db_path, db_id))
        return self.composite_score(symbol.upper(), records, run_epoch)

    def composite_score(
            self,
            symbol: str,
            articles: Iterable[Mapping[str, Any]],
            run_epoch: float) -> dict[str, Any]:
        """
        Compute the single composite ranking score for one symbol from
        per-article records adapted from live Bespin data.
        """
        weighted_positive = 0.0
        weighted_negative = 0.0
        weighted_neutral = 0.0
        n_eff = 0.0
        articles_total = 0
        articles_used = 0
        articles_skipped_no_timestamp = 0

        for article in articles:
            articles_total += 1

            normalized = self.normalize_article(article)
            published_epoch = normalized.get("published_epoch")
            if published_epoch is None:
                articles_skipped_no_timestamp += 1
                continue

            age_seconds = run_epoch - float(published_epoch)
            if age_seconds < 0.0:
                age_seconds = 0.0

            age_hours = age_seconds / SECONDS_PER_HOUR
            weight = 0.5 ** (age_hours / self.half_life_hours)

            weighted_positive += weight * float(normalized["positive_strength"])
            weighted_negative += weight * float(normalized["negative_strength"])
            weighted_neutral += weight * float(normalized["neutral_strength"])
            n_eff += weight
            articles_used += 1

        params = self.params()

        if n_eff == 0.0:
            self.composite_report = {
                "symbol": symbol,
                "state": "no_scoreable_articles",
                "composite_score": None,
                "polarity": None,
                "directional_density": None,
                "volume_factor": None,
                "n_eff": 0.0,
                "all_neutral": False,
                "articles_total": articles_total,
                "articles_used": 0,
                "articles_skipped_no_timestamp": articles_skipped_no_timestamp,
                "params": params,
            }
            return self.composite_report

        directional_mass = weighted_positive + weighted_negative
        if directional_mass == 0.0:
            polarity = 0.0
            all_neutral = True
        else:
            polarity = (weighted_positive - weighted_negative) / directional_mass
            all_neutral = False

        total_mass = directional_mass + weighted_neutral
        density = 0.0 if total_mass == 0.0 else directional_mass / total_mass
        volume_factor = n_eff / (n_eff + self.volume_shrinkage_k)
        score = polarity * (density ** self.density_exponent) * volume_factor
        state = (
            "insufficient_fresh_coverage"
            if n_eff < self.min_effective_volume
            else "scored"
        )

        self.composite_report = {
            "symbol": symbol,
            "state": state,
            "composite_score": round(score, 4),
            "polarity": round(polarity, 4),
            "directional_density": round(density, 4),
            "volume_factor": round(volume_factor, 4),
            "n_eff": round(n_eff, 2),
            "all_neutral": all_neutral,
            "articles_total": articles_total,
            "articles_used": articles_used,
            "articles_skipped_no_timestamp": articles_skipped_no_timestamp,
            "params": params,
        }
        return self.composite_report

    def iter_scoreable_articles(self, source: Any) -> Iterable[Mapping[str, Any]]:
        """Yield article-like dicts from Bespin dict/list/DataFrame sources."""
        if source is None:
            return

        if self._looks_like_dataframe(source):
            yield from self._dataframe_to_articles(source)
            return

        if isinstance(source, Mapping):
            yield source
            return

        if isinstance(source, Iterable) and not isinstance(source, (str, bytes)):
            for item in source:
                if isinstance(item, Mapping):
                    yield item
                elif self._looks_like_dataframe(item):
                    yield from self._dataframe_to_articles(item)
            return

        raise TypeError(f"Unsupported composite score source: {type(source)!r}")

    def normalize_article(self, article: Mapping[str, Any]) -> dict[str, float | None]:
        """
        Convert a template-style or Bespin-style article record to strengths.

        If explicit strengths are present, they win. Otherwise the method
        derives strengths from LMDB chunk sub-dicts:
          positive chunk -> positive_strength += sent_score
          neutral chunk  -> neutral_strength += sent_score
          negative chunk -> negative_strength += sent_score

        If no chunks are available, it falls back to root-level counts.
        """
        published_epoch = self.resolve_published_epoch(article)

        if self._has_explicit_strengths(article):
            return {
                "published_epoch": published_epoch,
                "positive_strength": self._to_float(article.get("positive_strength")),
                "neutral_strength": self._to_float(article.get("neutral_strength")),
                "negative_strength": self._to_float(article.get("negative_strength")),
            }

        positive_strength = 0.0
        neutral_strength = 0.0
        negative_strength = 0.0
        chunk_count = 0

        for chunk in self.iter_lmdb_chunks(article):
            sent_type = str(chunk.get("sent_type", chunk.get("sent", ""))).lower()
            sent_score = self._to_float(chunk.get("sent_score", chunk.get("rank", 1.0)))
            if sent_type == "positive":
                positive_strength += sent_score
            elif sent_type == "neutral":
                neutral_strength += sent_score
            elif sent_type == "negative":
                negative_strength += sent_score
            chunk_count += 1

        if chunk_count == 0:
            positive_strength = self._to_float(article.get("positive_count"))
            neutral_strength = self._to_float(article.get("neutral_count"))
            negative_strength = self._to_float(article.get("negative_count"))

        return {
            "published_epoch": published_epoch,
            "positive_strength": positive_strength,
            "neutral_strength": neutral_strength,
            "negative_strength": negative_strength,
        }

    def iter_lmdb_chunks(self, article: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """Yield chunk sub-dicts from a Bespin LMDB article package."""
        for key, value in article.items():
            if isinstance(value, Mapping) and self._is_chunk_key(key):
                yield value

    def resolve_published_epoch(self, article: Mapping[str, Any]) -> float | None:
        """
        Resolve article time from known Bespin/template fields.

        Priority:
          1. published_epoch
          2. nested age dicts with published_epoch
          3. ISO timestamp strings: iso_age, published_utc, published_at, skim_age
        """
        direct_epoch = self._to_float_or_none(article.get("published_epoch"))
        if direct_epoch is not None:
            return direct_epoch

        for key in ("age", "age0", "publish_age", "skim_age"):
            value = article.get(key)
            if isinstance(value, Mapping):
                nested_epoch = self._to_float_or_none(value.get("published_epoch"))
                if nested_epoch is not None:
                    return nested_epoch
                nested_iso = value.get("published_utc")
                parsed_nested = self._parse_datetime_epoch(nested_iso)
                if parsed_nested is not None:
                    return parsed_nested

        for key in ("iso_age", "published_utc", "published_at", "published"):
            parsed = self._parse_datetime_epoch(article.get(key))
            if parsed is not None:
                return parsed

        parsed_skim = self._parse_datetime_epoch(article.get("skim_age"))
        if parsed_skim is not None:
            return parsed_skim

        return None

    def load_symbol_articles_from_lmdb(
            self,
            symbol: str,
            db_path: str | Path = DEFAULT_LMDB_PATH,
            db_id: str = DEFAULT_DB_ID) -> Iterable[dict[str, Any]]:
        """Stream JSON article records for one ticker from Bespin's LMDB cache."""
        if lmdb is None:
            raise RuntimeError("lmdb is not installed; install requirements before reading LMDB.")

        symbol = symbol.upper()
        db_path = Path(db_path)
        prefix = f"{db_id}.{symbol}.".encode("utf-8")

        env = lmdb.open(
            str(db_path),
            readonly=True,
            lock=False,
            readahead=False,
            max_readers=126,
        )
        try:
            with env.begin() as txn:
                cursor = txn.cursor()
                if cursor.set_range(prefix):
                    for key, value in cursor:
                        if not key.startswith(prefix):
                            break
                        try:
                            record = json.loads(value.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                        if isinstance(record, dict):
                            yield record
        finally:
            env.close()

    def params(self) -> dict[str, float]:
        return {
            "half_life_hours": self.half_life_hours,
            "volume_shrinkage_k": self.volume_shrinkage_k,
            "density_exponent": self.density_exponent,
            "min_effective_volume": self.min_effective_volume,
        }

    def _dataframe_to_articles(self, dataframe: Any) -> Iterable[dict[str, Any]]:
        records = dataframe.to_dict(orient="records")
        if "urlhash" not in getattr(dataframe, "columns", []):
            yield from records
            return

        grouped: dict[Any, dict[str, Any]] = {}
        for row in records:
            urlhash = row.get("urlhash")
            article = grouped.setdefault(
                urlhash,
                {
                    "urlhash": urlhash,
                    "published_epoch": row.get("published_epoch"),
                    "iso_age": row.get("iso_age"),
                    "positive_strength": 0.0,
                    "neutral_strength": 0.0,
                    "negative_strength": 0.0,
                },
            )
            sent_type = str(row.get("snt", row.get("sent_type", ""))).lower()
            sent_score = self._to_float(row.get("rnk", row.get("sent_score", 1.0)))
            if sent_type == "positive":
                article["positive_strength"] += sent_score
            elif sent_type == "neutral":
                article["neutral_strength"] += sent_score
            elif sent_type == "negative":
                article["negative_strength"] += sent_score

        yield from grouped.values()

    @staticmethod
    def _looks_like_dataframe(source: Any) -> bool:
        return hasattr(source, "to_dict") and hasattr(source, "columns")

    @staticmethod
    def _is_chunk_key(key: Any) -> bool:
        key_text = str(key)
        return key_text.isdigit() and len(key_text) == 3

    @staticmethod
    def _has_explicit_strengths(article: Mapping[str, Any]) -> bool:
        keys = {"positive_strength", "neutral_strength", "negative_strength"}
        return any(key in article for key in keys)

    @staticmethod
    def _to_float(value: Any) -> float:
        parsed = CompositeScorer._to_float_or_none(value)
        return 0.0 if parsed is None or math.isnan(parsed) else parsed

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_datetime_epoch(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip()
        if text == "" or text.lower() == "none":
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute one Bespin composite sentiment score from LMDB."
    )
    parser.add_argument("symbol", help="Ticker symbol to score, e.g. WULF")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_LMDB_PATH),
        help="Path to LMDB environment directory. Default: datastore/LMDB_0001",
    )
    parser.add_argument(
        "--run-epoch",
        type=float,
        default=None,
        help="UTC epoch seconds anchor for this scoring run. Defaults to now.",
    )
    args = parser.parse_args()

    scorer = CompositeScorer()
    report = scorer.score_symbol_from_lmdb(args.symbol, args.db_path, args.run_epoch)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
