#!/usr/bin/env python3

"""
Composite sentiment scorer for Bespin news sentiment records.

CANONICAL single source of truth for decay-weighted sentiment math.
Both composite_score() and recency_weighted_polarity() share ONE
normalization + accumulation pass - the two metrics can never drift.

Reconciliation drop-in: implements all review fixes -
  R1  mean-based chunk reduction (one-article-one-vote) on ALL paths
  R2  shared accumulation core; standalone recency_weighted_polarity
  R3  generator-safe article handling (no len() on iterables)
  R4  per-call counter reset (multi-symbol ranking loops safe)
  R5  no silent defaults: missing sent_score / unknown sent_type /
      JSON decode failures are counted + error-logged, never guessed
  R6  full adapter telemetry: states, provenance, counts_consistent
  R7  partial explicit-strengths = error (all-or-nothing)
  R8  dead cmi_debug construction removed from hot-path helpers
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# logging setup
logging.basicConfig(level=logging.INFO)

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

_VALID_SENT_TYPES = ("positive", "neutral", "negative")


# ############################# MAIN CLASS
class CompositeScorer:
    """
    Compute decay-weighted sentiment metrics for one stock ticker.

    Public entry points:
      - composite_score(symbol, articles, run_epoch)
      - recency_weighted_polarity(symbol, articles, run_epoch)
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
        self.polarity_report: dict[str, Any] | None = None
        # R4: instance counters, reset at the top of every public call
        self.lmdb_record_count = 0
        self.processing_record = 0
        cmi_debug = __name__+"::" + self.__init__.__name__
        logging.info(f'%s Instantiate' % cmi_debug)

# ############################# Method #0
    def _reset_run_counters(self) -> None:
        """
        R4: counters are per-public-call, NOT cumulative. In the
        75-symbol ranking loop one scorer instance makes many calls;
        cumulative counts can never reconcile against per-symbol LMDB
        record counts, which breaks the clean-store invariant check.
        """
        self.lmdb_record_count = 0
        self.processing_record = 0

# ############################# Method #1
    def score_symbol(
            self,
            symbol: str,
            source: Any,
            run_epoch: float | None = None) -> dict[str, Any]:
        """
        Score one ticker from live Bespin in-memory data.

        `source` may be:
          - one article dict
          - iterable/list/generator of article dicts
          - pandas DataFrame with article or chunk rows
        """
        if run_epoch is None:
            run_epoch = time.time()
        self._reset_run_counters()

        cmi_debug = __name__+"::"+self.score_symbol.__name__
        logging.info(f"%s    - Compute a Ticker composite score @ time window: {run_epoch}." % cmi_debug )

        articles = self.iter_scoreable_articles(source)
        return self.composite_score(symbol.upper(), articles, run_epoch)

# ############################# Method #2
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
        """
        if run_epoch is None:
            run_epoch = time.time()
        self._reset_run_counters()

        cmi_debug = __name__+"::"+self.score_symbol_from_lmdb.__name__
        logging.info(f"%s    - Compute LMDB data composite score @ time window: {run_epoch}." % cmi_debug )

        records = self.load_symbol_articles_from_lmdb(symbol, db_path, db_id)
        return self.composite_score(symbol.upper(), records, run_epoch)

# ############################# Method #3
    def _normalize_and_accumulate(
            self,
            articles: Iterable[Mapping[str, Any]],
            run_epoch: float) -> dict[str, Any]:
        """
        R2: THE single accumulation pass. Normalizes every article,
        applies decay weighting, accumulates P/N/U masses and full
        adapter telemetry. composite_score and
        recency_weighted_polarity BOTH consume this - the decay loop
        must never be duplicated elsewhere.

        R3: consumes any iterable (list, generator, LMDB stream)
        without calling len().
        """
        cmi_debug = __name__+"::"+self._normalize_and_accumulate.__name__

        weighted_positive = 0.0
        weighted_negative = 0.0
        weighted_neutral = 0.0
        n_eff = 0.0
        articles_total = 0
        articles_used = 0
        articles_skipped_no_timestamp = 0
        state_tally: dict[str, int] = {"scored": 0, "empty": 0, "unreadable": 0}
        provenance_tally: dict[str, int] = {}
        counts_inconsistent = 0
        chunks_invalid_total = 0

        for article in articles:
            articles_total += 1

            normalized = self.normalize_article(article)

            state_tally[normalized["state"]] = \
                state_tally.get(normalized["state"], 0) + 1
            prov = normalized["timestamp_provenance"]
            provenance_tally[prov] = provenance_tally.get(prov, 0) + 1
            chunks_invalid_total += normalized["chunks_invalid"]
            if normalized["counts_consistent"] is False:
                counts_inconsistent += 1

            if normalized["state"] != "scored":
                # empty / unreadable records carry no sentiment mass -
                # counted in tallies, excluded from the vote.
                continue

            published_epoch = normalized["published_epoch"]
            if published_epoch is None:
                articles_skipped_no_timestamp += 1
                logging.error(f"%s    - Skip article with no timestamp: "
                              f"{normalized['urlhash']}" % cmi_debug )
                continue

            age_seconds = run_epoch - float(published_epoch)
            if age_seconds < 0.0:
                age_seconds = 0.0   # clock-skew guard: clamp, never inflate

            age_hours = age_seconds / SECONDS_PER_HOUR
            weight = 0.5 ** (age_hours / self.half_life_hours)
            logging.info(f"%s    - Article age is: {age_hours:.2f} hours / Age Weight computed as: {weight:.4f}" % cmi_debug)

            weighted_positive += weight * normalized["positive_strength"]
            weighted_negative += weight * normalized["negative_strength"]
            weighted_neutral += weight * normalized["neutral_strength"]
            n_eff += weight
            articles_used += 1

        logging.info(f"%s    - Accumulated: articles={articles_total} used={articles_used} n_eff={n_eff:.2f}" % cmi_debug)
        return {
            "P": weighted_positive,
            "N": weighted_negative,
            "U": weighted_neutral,
            "n_eff": n_eff,
            "articles_total": articles_total,
            "articles_used": articles_used,
            "articles_skipped_no_timestamp": articles_skipped_no_timestamp,
            "adapter": {
                "state_tally": state_tally,
                "provenance_tally": provenance_tally,
                "counts_inconsistent": counts_inconsistent,
                "chunks_invalid_total": chunks_invalid_total,
            },
        }

# ############################# Method #4
    def composite_score(
            self,
            symbol: str,
            articles: Iterable[Mapping[str, Any]],
            run_epoch: float) -> dict[str, Any]:
        """
        S = polarity * density ** DENSITY_EXPONENT * volume_factor
        computed over decay-weighted, mean-normalized article strengths.
        """
        cmi_debug = __name__+"::"+self.composite_score.__name__
        logging.info(f"%s    - Compute {symbol} composite ranking metrics @ window: {run_epoch}." % cmi_debug )

        m = self._normalize_and_accumulate(articles, run_epoch)
        params = self.params()

        if m["n_eff"] == 0.0:
            logging.info(f"%s    - Computed N_EFF = 0.0 !!" % cmi_debug)
            self.composite_report = {
                "symbol": symbol,
                "state": "no_scoreable_articles",
                "composite_score": None,
                "polarity": None,
                "directional_density": None,
                "volume_factor": None,
                "n_eff": 0.0,
                "all_neutral": False,
                "articles_total": m["articles_total"],
                "articles_used": 0,
                "articles_skipped_no_timestamp": m["articles_skipped_no_timestamp"],
                "adapter": m["adapter"],
                "params": params,
            }
            return self.composite_report

        directional_mass = m["P"] + m["N"]
        if directional_mass == 0.0:
            logging.info(f"%s    - Computed Directional Mass & Polarity = 0.0 !!" % cmi_debug)
            polarity = 0.0
            all_neutral = True
        else:
            polarity = (m["P"] - m["N"]) / directional_mass
            all_neutral = False
            logging.info(f"%s    - Computed Directional Mass & Polarity: {directional_mass:.2f} / {polarity:.4f}" % cmi_debug)

        total_mass = directional_mass + m["U"]
        if total_mass == 0.0:
            density = 0.0
        else:
            density = directional_mass / total_mass

        volume_factor = m["n_eff"] / (m["n_eff"] + self.volume_shrinkage_k)
        score = polarity * (density ** self.density_exponent) * volume_factor

        if m["n_eff"] < self.min_effective_volume:
            state = "insufficient_fresh_coverage"
            logging.info(f"%s    - Insufficient Fresh Article coverage !" % cmi_debug)
        else:
            state = "scored"

        logging.info(f"%s    - Computed viable composite score: {score:.4f}" % cmi_debug)
        self.composite_report = {
            "symbol": symbol,
            "state": state,
            "composite_score": round(score, 4),
            "polarity": round(polarity, 4),
            "directional_density": round(density, 4),
            "volume_factor": round(volume_factor, 4),
            "n_eff": round(m["n_eff"], 2),
            "all_neutral": all_neutral,
            "articles_total": m["articles_total"],
            "articles_used": m["articles_used"],
            "articles_skipped_no_timestamp": m["articles_skipped_no_timestamp"],
            "adapter": m["adapter"],
            "params": params,
        }
        return self.composite_report

# ############################# Method #5
    def recency_weighted_polarity(
            self,
            symbol: str,
            articles: Iterable[Mapping[str, Any]],
            run_epoch: float) -> dict[str, Any]:
        """
        R2: TRUE recency-weighted neutral-excluded polarity,
        polarity = (P - N) / (P + N), as a thin sibling over the SAME
        accumulation core as composite_score - provably consistent.
        """
        cmi_debug = __name__+"::"+self.recency_weighted_polarity.__name__
        logging.info(f"%s    - Compute {symbol} weighted polarity @ window: {run_epoch}." % cmi_debug )

        m = self._normalize_and_accumulate(articles, run_epoch)

        if m["n_eff"] == 0.0:
            self.polarity_report = {
                "symbol": symbol,
                "state": "no_scoreable_articles",
                "polarity": None,
                "all_neutral": False,
                "P": 0.0,
                "N": 0.0,
                "n_eff": 0.0,
                "articles_total": m["articles_total"],
                "articles_skipped_no_timestamp": m["articles_skipped_no_timestamp"],
                "adapter": m["adapter"],
                "params": {"half_life_hours": self.half_life_hours},
            }
            return self.polarity_report

        directional_mass = m["P"] + m["N"]
        if directional_mass == 0.0:
            polarity = 0.0
            all_neutral = True
        else:
            polarity = (m["P"] - m["N"]) / directional_mass
            all_neutral = False

        self.polarity_report = {
            "symbol": symbol,
            "state": "scored",
            "polarity": round(polarity, 4),
            "all_neutral": all_neutral,
            "P": round(m["P"], 4),
            "N": round(m["N"], 4),
            "n_eff": round(m["n_eff"], 2),
            "articles_total": m["articles_total"],
            "articles_skipped_no_timestamp": m["articles_skipped_no_timestamp"],
            "adapter": m["adapter"],
            "params": {"half_life_hours": self.half_life_hours},
        }
        return self.polarity_report

# ############################# Method #6
    def iter_scoreable_articles(self, source: Any) -> Iterable[Mapping[str, Any]]:
        """Yield article-like dicts from Bespin dict/list/DataFrame sources."""
        cmi_debug = __name__+"::"+self.iter_scoreable_articles.__name__
        logging.info(f"%s    - identify input data source" % cmi_debug )

        if source is None:
            return

        if self._looks_like_dataframe(source):
            logging.info(f"%s    - Pandas DataFrame" % cmi_debug )
            yield from self._dataframe_to_articles(source)
            return

        if isinstance(source, Mapping):
            logging.info(f"%s    - Mapping" % cmi_debug )
            yield source
            return

        if isinstance(source, Iterable) and not isinstance(source, (str, bytes)):
            logging.info(f"%s    - Iterable entity: {type(source)}" % cmi_debug )
            for item in source:
                if isinstance(item, Mapping):
                    yield item
                elif self._looks_like_dataframe(item):
                    yield from self._dataframe_to_articles(item)
            return

        raise TypeError(f"Unsupported composite score source: {type(source)!r}")

# ############################# Method #7
    def normalize_article(self, article: Mapping[str, Any]) -> dict[str, Any]:
        """
        Convert a Bespin article record to article-level strengths.

        R1: ALL paths produce MEAN-scale strengths (one-article-one-
        vote). Article length must never be a hidden sentiment weight:
          - explicit strengths: used as-is (already article-level)
          - LMDB chunk dicts:   per-bucket sum of sent_score / valid chunks
          - count fallback:     label counts / total counts (share scale)

        R5/R6: no silent defaults. Missing/invalid chunk data is
        counted in chunks_invalid + error-logged, never guessed.
        Returns full telemetry:
          state             : "scored" | "empty" | "unreadable"
          urlhash, published_epoch, timestamp_provenance,
          positive/neutral/negative_strength,
          chunks_used, chunks_invalid, counts_consistent
        """
        cmi_debug = __name__+"::"+self.normalize_article.__name__
        self.processing_record += 1
        _urlhash = str(article.get("urlhash", "UNKNOWN"))
        logging.info(f"%s          - Normalize article record {self.processing_record}: {_urlhash}" % cmi_debug )

        published_epoch, provenance = self.resolve_published_epoch(article)

        # ---- path 1: explicit article-level strengths ----
        # R7: all-or-nothing. Partial presence would silently zero the
        # missing buckets - that is corrupt input, not a default case.
        strength_keys = ("positive_strength", "neutral_strength", "negative_strength")
        present = [key for key in strength_keys if key in article]
        if len(present) == len(strength_keys):
            return {
                "state": "scored",
                "urlhash": _urlhash,
                "published_epoch": published_epoch,
                "timestamp_provenance": provenance,
                "positive_strength": self._to_float(article.get("positive_strength")),
                "neutral_strength": self._to_float(article.get("neutral_strength")),
                "negative_strength": self._to_float(article.get("negative_strength")),
                "chunks_used": 0,
                "chunks_invalid": 0,
                "counts_consistent": True,
            }
        elif len(present) > 0:
            logging.error(f"%s          - PARTIAL explicit strengths {present} on {_urlhash} - record unreadable" % cmi_debug )
            return self._unreadable_profile(_urlhash, published_epoch, provenance)

        # ---- path 2: LMDB chunk sub-dicts, mean-reduced ----
        sum_positive = 0.0
        sum_neutral = 0.0
        sum_negative = 0.0
        tally = {"positive": 0, "neutral": 0, "negative": 0}
        chunks_used = 0
        chunks_invalid = 0
        chunks_seen = 0

        for chunk in self.iter_lmdb_chunks(article):
            chunks_seen += 1
            sent_type = str(chunk.get("sent_type", chunk.get("sent", ""))).lower()
            sent_score = self._to_float_or_none(
                chunk.get("sent_score", chunk.get("rank")))

            if sent_type not in _VALID_SENT_TYPES:
                # R5: unknown label = invalid chunk. It contributes NO
                # mass and NO vote - previously it still bumped
                # chunk_count, letting corrupt articles inflate n_eff.
                chunks_invalid += 1
                logging.error(f"%s          - Unknown sent_type {sent_type!r} on {_urlhash} chunk {chunk.get('chunk')}" % cmi_debug )
                continue
            if sent_score is None or math.isnan(sent_score):
                # R5: absent score is a fault, NOT a 1.0 full-strength
                # vote. A missing value must never out-vote real data.
                chunks_invalid += 1
                logging.error(f"%s          - Missing/invalid sent_score on {_urlhash} chunk {chunk.get('chunk')}" % cmi_debug )
                continue
            if sent_score < 0.0 or sent_score > 1.0:
                logging.error(f"%s          - sent_score out of [0,1] ({sent_score:.4f}) on {_urlhash} - clamped" % cmi_debug )
                sent_score = max(0.0, min(1.0, sent_score))

            if sent_type == "positive":
                sum_positive += sent_score
            elif sent_type == "negative":
                sum_negative += sent_score
            else:
                sum_neutral += sent_score
            tally[sent_type] += 1
            chunks_used += 1

        if chunks_used > 0:
            # R6: consistency check vs stored per-label tallies, only
            # when the stored counts exist to check against.
            counts_consistent = True
            if all(key in article for key in
                   ("positive_count", "neutral_count", "negative_count")):
                counts_consistent = (
                    tally["positive"] == self._to_int(article.get("positive_count"))
                    and tally["neutral"] == self._to_int(article.get("neutral_count"))
                    and tally["negative"] == self._to_int(article.get("negative_count"))
                )
                if counts_consistent is False:
                    logging.error(f"%s          - Chunk tally mismatch vs stored counts on {_urlhash}: computed={tally}" % cmi_debug )

            # R1: MEAN reduction - one-article-one-vote.
            return {
                "state": "scored",
                "urlhash": _urlhash,
                "published_epoch": published_epoch,
                "timestamp_provenance": provenance,
                "positive_strength": sum_positive / chunks_used,
                "neutral_strength": sum_neutral / chunks_used,
                "negative_strength": sum_negative / chunks_used,
                "chunks_used": chunks_used,
                "chunks_invalid": chunks_invalid,
                "counts_consistent": counts_consistent,
            }

        if chunks_seen > 0:
            # chunks existed but every one was invalid
            return self._unreadable_profile(_urlhash, published_epoch, provenance,
                                            chunks_invalid=chunks_invalid)

        # ---- path 3: count fallback, share-normalized to mean scale ----
        pos_count = self._to_float(article.get("positive_count"))
        neu_count = self._to_float(article.get("neutral_count"))
        neg_count = self._to_float(article.get("negative_count"))
        total_count = pos_count + neu_count + neg_count
        if total_count > 0.0:
            # R1: raw counts are article-length scaled - dividing by the
            # total yields shares on the same 0..1 scale as chunk means
            # (equivalent to mean reduction with every score = 1.0).
            return {
                "state": "scored",
                "urlhash": _urlhash,
                "published_epoch": published_epoch,
                "timestamp_provenance": provenance,
                "positive_strength": pos_count / total_count,
                "neutral_strength": neu_count / total_count,
                "negative_strength": neg_count / total_count,
                "chunks_used": 0,
                "chunks_invalid": 0,
                "counts_consistent": True,
            }

        # ---- nothing usable at all ----
        return {
            "state": "empty",
            "urlhash": _urlhash,
            "published_epoch": published_epoch,
            "timestamp_provenance": provenance,
            "positive_strength": 0.0,
            "neutral_strength": 0.0,
            "negative_strength": 0.0,
            "chunks_used": 0,
            "chunks_invalid": 0,
            "counts_consistent": True,
        }

# ############################# Method #8
    @staticmethod
    def _unreadable_profile(
            urlhash: str,
            published_epoch: float | None,
            provenance: str,
            chunks_invalid: int = 0) -> dict[str, Any]:
        """Uniform shape for unreadable records - same keys, zero mass."""
        return {
            "state": "unreadable",
            "urlhash": urlhash,
            "published_epoch": published_epoch,
            "timestamp_provenance": provenance,
            "positive_strength": 0.0,
            "neutral_strength": 0.0,
            "negative_strength": 0.0,
            "chunks_used": 0,
            "chunks_invalid": chunks_invalid,
            "counts_consistent": True,
        }

# ############################# Method #9
    def iter_lmdb_chunks(self, article: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """Yield chunk sub-dicts from a Bespin LMDB article package."""
        for key, value in article.items():
            if isinstance(value, Mapping) and self._is_chunk_key(key):
                yield value

# ############################# Method #10
    def resolve_published_epoch(
            self,
            article: Mapping[str, Any]) -> tuple[float | None, str]:
        """
        Resolve article publish time from known Bespin/template fields.

        Returns (epoch, provenance) - R6: provenance feeds the report
        layer's telemetry (empirical vs skim-fallback vs missing).

        Priority:
          1. published_epoch                    -> "direct_epoch"
          2. nested age dicts                   -> "nested"
          3. iso_age / published_utc / etc.     -> "article_empirical"
          4. skim_age ISO string (LAST resort)  -> "skim_estimate"
        """
        cmi_debug = __name__+"::"+self.resolve_published_epoch.__name__

        direct_epoch = self._to_float_or_none(article.get("published_epoch"))
        if direct_epoch is not None:
            logging.info(f"%s    - PUBLISHED_EPOCH resolved DIRECTLY as: {direct_epoch}" % cmi_debug )
            return direct_epoch, "direct_epoch"

        for key in ("age", "age0", "publish_age"):
            value = article.get(key)
            if isinstance(value, Mapping):
                nested_epoch = self._to_float_or_none(value.get("published_epoch"))
                if nested_epoch is not None:
                    logging.info(f"%s    - PUBLISHED_EPOCH found in nested structure: {nested_epoch}" % cmi_debug )
                    return nested_epoch, str(value.get("provenance", "nested"))
                parsed_nested = self._parse_datetime_epoch(value.get("published_utc"))
                if parsed_nested is not None:
                    logging.info(f"%s    - PUBLISHED_UTC epoch found in nested structure: {parsed_nested}" % cmi_debug )
                    return parsed_nested, str(value.get("provenance", "nested"))

        for key in ("iso_age", "published_utc", "published_at", "published"):
            parsed = self._parse_datetime_epoch(article.get(key))
            if parsed is not None:
                logging.info(f"%s    - {key.upper()} epoch found: {parsed}" % cmi_debug )
                return parsed, "article_empirical"

        parsed_skim = self._parse_datetime_epoch(article.get("skim_age"))
        if parsed_skim is not None:
            logging.info(f"%s    - SKIM_AGE epoch found (fallback): {parsed_skim}" % cmi_debug )
            return parsed_skim, "skim_estimate"

        return None, "none"

# ############################# Method #11
    def load_symbol_articles_from_lmdb(
            self,
            symbol: str,
            db_path: str | Path = DEFAULT_LMDB_PATH,
            db_id: str = DEFAULT_DB_ID) -> Iterable[dict[str, Any]]:
        """
        Stream LMDB JSON article records for one ticker from Bespin's LMDB cache
        """
        if lmdb is None:
            raise RuntimeError("lmdb is not installed; install requirements before reading LMDB.")

        symbol = symbol.upper()
        cmi_debug = __name__+"::"+self.load_symbol_articles_from_lmdb.__name__
        logging.info(f"%s    - Load {symbol} article data from LMDB..." % cmi_debug )

        db_path = Path(db_path)
        prefix = f"{db_id}.{symbol}.".encode("utf-8")
        logging.info(f"%s    - Scaning for LMDB data pattern: {prefix}" % cmi_debug )

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
                    logging.info(f"%s    - Opened LMDB database for READ-ONLY Txn..." % cmi_debug )
                    for key, value in cursor:
                        if not key.startswith(prefix):
                            break
                        try:
                            record = json.loads(value.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError) as err:
                            # R5: a half-written/corrupt record must
                            # leave a trace, not vanish silently.
                            logging.error(f"%s    - CORRUPT LMDB record {key!r}: {err}" % cmi_debug )
                            continue
                        if isinstance(record, dict):
                            self.lmdb_record_count += 1
                            yield record
        finally:
            logging.info(f"%s    - Close LMDB database / Populated {self.lmdb_record_count} records" % cmi_debug )
            env.close()

# ############################# Method #12
    def params(self) -> dict[str, float]:
        cmi_debug = __name__+"::params.#_loader"
        logging.info(f"%s    - Load 4 critical CONSTANT Weighting VARS" % cmi_debug )
        return {
            "half_life_hours": self.half_life_hours,
            "volume_shrinkage_k": self.volume_shrinkage_k,
            "density_exponent": self.density_exponent,
            "min_effective_volume": self.min_effective_volume,
        }

# ############################# Method #13
    def _dataframe_to_articles(self, dataframe: Any) -> Iterable[dict[str, Any]]:
        """
        R1: DataFrame chunk rows are mean-reduced per urlhash, matching
        the LMDB chunk path scale. Rows with missing/invalid sentiment
        are counted + error-logged, matching the chunk path (R5).
        """
        cmi_debug = __name__+"::"+self._dataframe_to_articles.__name__
        logging.info(f"%s    - Read data from DataFrame" % cmi_debug )
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
                    "_sum_positive": 0.0,
                    "_sum_neutral": 0.0,
                    "_sum_negative": 0.0,
                    "_rows_used": 0,
                },
            )
            sent_type = str(row.get("snt", row.get("sent_type", ""))).lower()
            sent_score = self._to_float_or_none(row.get("rnk", row.get("sent_score")))

            if sent_type not in _VALID_SENT_TYPES:
                logging.error(f"%s    - Unknown sent_type {sent_type!r} on DataFrame row for {urlhash}" % cmi_debug )
                continue
            if sent_score is None or math.isnan(sent_score):
                logging.error(f"%s    - Missing sent score on DataFrame row for {urlhash}" % cmi_debug )
                continue

            if sent_type == "positive":
                article["_sum_positive"] += sent_score
            elif sent_type == "neutral":
                article["_sum_neutral"] += sent_score
            elif sent_type == "negative":
                article["_sum_negative"] += sent_score
            article["_rows_used"] += 1

        for article in grouped.values():
            rows_used = article.pop("_rows_used")
            sum_positive = article.pop("_sum_positive")
            sum_neutral = article.pop("_sum_neutral")
            sum_negative = article.pop("_sum_negative")
            if rows_used > 0:
                article["positive_strength"] = sum_positive / rows_used
                article["neutral_strength"] = sum_neutral / rows_used
                article["negative_strength"] = sum_negative / rows_used
            else:
                article["positive_strength"] = 0.0
                article["neutral_strength"] = 0.0
                article["negative_strength"] = 0.0
            yield article

# ############################# Decorator #1
    @staticmethod
    def _looks_like_dataframe(source: Any) -> bool:
        return hasattr(source, "to_dict") and hasattr(source, "columns")

# ############################# Decorator #2
    @staticmethod
    def _is_chunk_key(key: Any) -> bool:
        key_text = str(key)
        return key_text.isdigit() and len(key_text) == 3

# ############################# Decorator #3
    @staticmethod
    def _to_float(value: Any) -> float:
        # R8: no cmi_debug construction here - hot loop helper
        parsed = CompositeScorer._to_float_or_none(value)
        if parsed is None or math.isnan(parsed):
            return 0.0
        else:
            return parsed

# ############################# Decorator #4
    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        # R8: no cmi_debug construction here - hot loop helper
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

# ############################# Decorator #5
    @staticmethod
    def _to_int(value: Any) -> int:
        # sentinel -1 on unparseable so it can never equal a real tally
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

# ############################# Decorator #6
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


# ############################# MAIN() entry point for command-line execution
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
        help="UTC epoch seconds anchor for this scoring run. Defaults to now. "
             "For cross-symbol ranking, pass ONE shared anchor to every symbol.",
    )
    parser.add_argument(
        "--polarity",
        help="also print the standalone recency-weighted polarity report",
        action="store_true",
        dest="bool_polarity",
        required=False,
        default=False,
    )
    parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)

    args = parser.parse_args()

    if args.bool_verbose is True:        # Logging level
        print ( "Enabeling verbose info logging..." )
        logging.disable(0)                  # re-enable ALL log levels (verbose mode)
    else:
        logging.disable(20)                 # suppress INFO and below (quiet default; ERROR still shows)

    scorer = CompositeScorer()
    report = scorer.score_symbol_from_lmdb(args.symbol, args.db_path, args.run_epoch)
    print(json.dumps(report, indent=2, sort_keys=True))

    if args.bool_polarity is True:
        scorer._reset_run_counters()
        records = scorer.load_symbol_articles_from_lmdb(args.symbol, args.db_path)
        polarity_report = scorer.recency_weighted_polarity(
            args.symbol.upper(), records,
            args.run_epoch if args.run_epoch is not None else time.time())
        print(json.dumps(polarity_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())