#!/usr/bin/env python3

"""
Supabase publisher for Bespin composite-score evidence.

The writer follows the schema in webux/supabase_build.txt:
  * articles are first-writer-wins by full 64-char urlhash
  * heatmap_runs are append-only and unique per instance/symbol/run_epoch
  * heatmap_rows store the exact per-run voting weights
  * article_divergences records article-strength disagreements
  * triggers can carry the computed news/price divergence payload

Credentials are intentionally read from environment variables. Do not
commit Supabase service-role secrets to this repository.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


SECONDS_PER_HOUR = 3600.0


@dataclass(frozen=True)
class SupabasePublishResult:
    """Small execution summary for the CLI/report layer."""

    dry_run: bool
    run_id: int | None
    articles_seen: int
    articles_publishable: int
    rows_publishable: int
    divergences_logged: int
    triggers_logged: int
    skipped: int


class SupabaseEngine:
    """HTTP/PostgREST client for Bespin's Supabase evidence tables."""

    def __init__(
        self,
        *,
        supabase_url: str | None = None,
        service_role_key: str | None = None,
        instance_id: str | None = None,
        divergence_epsilon: float = 0.001,
        timeout_seconds: float = 30.0,
        dry_run: bool = False,
    ) -> None:
        self.supabase_url = (
            supabase_url
            or os.getenv("BESPIN_SUPABASE_URL")
            or os.getenv("SUPABASE_URL")
        )
        self.service_role_key = (
            service_role_key
            or os.getenv("BESPIN_SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        )
        self.instance_id = (
            instance_id
            or os.getenv("BESPIN_INSTANCE_ID")
            or os.getenv("BESPIN_SUPABASE_INSTANCE_ID")
        )
        self.divergence_epsilon = divergence_epsilon
        self.timeout_seconds = timeout_seconds
        self.dry_run = dry_run

        if not self.instance_id:
            raise ValueError(
                "Supabase publish requires BESPIN_INSTANCE_ID or --supabase-instance-id."
            )
        if not self.dry_run and not self.supabase_url:
            raise ValueError("Supabase publish requires BESPIN_SUPABASE_URL.")
        if not self.dry_run and not self.service_role_key:
            raise ValueError("Supabase publish requires BESPIN_SUPABASE_SERVICE_ROLE_KEY.")

        if self.supabase_url:
            self.supabase_url = self.supabase_url.rstrip("/")

    def publish_run(
        self,
        *,
        symbol: str,
        run_epoch: float,
        records: Sequence[Mapping[str, Any]],
        scorer: Any,
        composite_report: Mapping[str, Any],
        polarity_report: Mapping[str, Any] | None = None,
        divergence_result: Any | None = None,
        bespin_version: str | None = None,
    ) -> SupabasePublishResult:
        """Publish one composite score run and its article memberships."""

        symbol = symbol.upper().strip()
        materialized = self._materialize_articles(symbol, run_epoch, records, scorer)
        article_rows = self._dedupe_by_urlhash(
            [item["article"] for item in materialized if item["article"]]
        )
        heatmap_seed_rows = self._dedupe_by_urlhash(
            [item["heatmap"] for item in materialized if item["heatmap"]]
        )
        tag_tally = self._tag_tally(article_rows)
        divergence_rows = self._build_article_divergences(article_rows)

        trigger_row = self._build_divergence_trigger(
            symbol=symbol,
            run_epoch=run_epoch,
            divergence_result=divergence_result,
        )

        if self.dry_run:
            return SupabasePublishResult(
                dry_run=True,
                run_id=None,
                articles_seen=len(records),
                articles_publishable=len(article_rows),
                rows_publishable=len(heatmap_seed_rows),
                divergences_logged=len(divergence_rows),
                triggers_logged=1 if trigger_row else 0,
                skipped=len(records) - len(article_rows),
            )

        if article_rows:
            self._upsert_articles(article_rows)
        if divergence_rows:
            self._insert_rows("article_divergences", divergence_rows)

        run_id = self._insert_run(
            symbol=symbol,
            run_epoch=run_epoch,
            composite_report=composite_report,
            polarity_report=polarity_report,
            bespin_version=bespin_version,
            tag_tally=tag_tally,
        )

        heatmap_rows = [
            {
                "run_id": run_id,
                "urlhash": row["urlhash"],
                "age_seconds": row["age_seconds"],
                "weight": row["weight"],
            }
            for row in heatmap_seed_rows
        ]
        if heatmap_rows:
            self._insert_rows("heatmap_rows", heatmap_rows)
        if trigger_row:
            self._insert_rows("triggers", [trigger_row])

        return SupabasePublishResult(
            dry_run=False,
            run_id=run_id,
            articles_seen=len(records),
            articles_publishable=len(article_rows),
            rows_publishable=len(heatmap_rows),
            divergences_logged=len(divergence_rows),
            triggers_logged=1 if trigger_row else 0,
            skipped=len(records) - len(article_rows),
        )

    def _materialize_articles(
        self,
        symbol: str,
        run_epoch: float,
        records: Sequence[Mapping[str, Any]],
        scorer: Any,
    ) -> list[dict[str, Any]]:
        materialized = []
        for record in records:
            profile = scorer.normalize_article(record)
            urlhash = str(profile.get("urlhash", "")).strip()
            published_epoch = self._optional_float(profile.get("published_epoch"))

            if len(urlhash) != 64 or published_epoch is None:
                materialized.append({"article": None, "heatmap": None})
                continue

            age_seconds = max(0.0, run_epoch - published_epoch)
            weight = None
            if profile.get("state") == "scored":
                age_hours = age_seconds / SECONDS_PER_HOUR
                weight = 0.5 ** (age_hours / float(scorer.half_life_hours))

            article = {
                "urlhash": urlhash,
                "symbol": symbol,
                "url": self._first_text(
                    record,
                    "url",
                    "exturl",
                    "Ext_url",
                    "article_url",
                    "source_url",
                    "link",
                ),
                "published_at": self._iso_from_epoch(published_epoch),
                "published_epoch": published_epoch,
                "sent_tag": self._sent_tag(profile),
                "pos_strength": self._round_or_none(profile.get("positive_strength")),
                "neu_strength": self._round_or_none(profile.get("neutral_strength")),
                "neg_strength": self._round_or_none(profile.get("negative_strength")),
                "chunk_count": self._chunk_count(record, profile),
                "first_seen_by": self.instance_id,
            }
            heatmap = {
                "urlhash": urlhash,
                "age_seconds": round(age_seconds, 6),
                "weight": self._round_or_none(weight),
            }
            materialized.append({"article": article, "heatmap": heatmap})
        return materialized

    def _build_article_divergences(
        self,
        article_rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        if not article_rows:
            return []

        existing = self._fetch_existing_articles([row["urlhash"] for row in article_rows])
        divergences = []
        for row in article_rows:
            stored = existing.get(str(row["urlhash"]))
            if not stored:
                continue
            if not self._strengths_differ(row, stored):
                continue
            divergences.append(
                {
                    "urlhash": row["urlhash"],
                    "reported_by": self.instance_id,
                    "pos_strength": row["pos_strength"],
                    "neu_strength": row["neu_strength"],
                    "neg_strength": row["neg_strength"],
                    "chunk_count": row["chunk_count"],
                }
            )
        return divergences

    # ###########################
    # prepare and buuld the final data package
    #
    def _build_divergence_trigger(
        self,
        *,
        symbol: str,
        run_epoch: float,
        divergence_result: Any | None,
    ) -> dict[str, Any] | None:
        if divergence_result is None:
            return None
        payload = (
            divergence_result.to_dict()
            if hasattr(divergence_result, "to_dict")
            else dict(divergence_result)
        )
        return {
            "instance_id": self.instance_id,
            "symbol": symbol,
            "triggered_at": self._iso_from_epoch(run_epoch),
            "trigger_type": "news_price_divergence",
            "payload": payload,
        }

    def _insert_run(
        self,
        *,
        symbol: str,
        run_epoch: float,
        composite_report: Mapping[str, Any],
        polarity_report: Mapping[str, Any] | None,
        bespin_version: str | None,
        tag_tally: Mapping[str, int],
    ) -> int:
        row = {
            "instance_id": self.instance_id,
            "bespin_version": bespin_version or os.getenv("BESPIN_VERSION"),
            "symbol": symbol,
            "run_epoch": run_epoch,
            "run_at": self._iso_from_epoch(run_epoch),
            "composite_score": self._round_or_none(composite_report.get("composite_score")),
            "polarity": self._round_or_none(composite_report.get("polarity")),
            "directional_density": self._round_or_none(
                composite_report.get("directional_density")
            ),
            "volume_factor": self._round_or_none(composite_report.get("volume_factor")),
            "n_eff": self._round_or_none(composite_report.get("n_eff")) or 0.0,
            "state": composite_report.get("state"),
            "articles_total": int(composite_report.get("articles_total") or 0),
            "articles_used": int(composite_report.get("articles_used") or 0),
            "pos_count": tag_tally.get("Pos"),
            "neu_count": tag_tally.get("Neu"),
            "neg_count": tag_tally.get("Neg"),
            "params": composite_report.get("params", {}),
        }
        if polarity_report:
            row["params"] = dict(row["params"])
            row["params"]["polarity_params"] = polarity_report.get("params", {})

        result = self._request(
            "POST",
            "heatmap_runs",
            json_payload=[row],
            prefer="return=representation",
        )
        if not result or "run_id" not in result[0]:
            raise RuntimeError("Supabase did not return run_id for heatmap_runs insert.")
        return int(result[0]["run_id"])

    def _upsert_articles(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._request(
            "POST",
            "articles",
            query={"on_conflict": "urlhash"},
            json_payload=list(rows),
            prefer="resolution=ignore-duplicates,return=minimal",
        )

    def _insert_rows(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        self._request(
            "POST",
            table,
            json_payload=list(rows),
            prefer="return=minimal",
        )

    def _fetch_existing_articles(self, urlhashes: Sequence[str]) -> dict[str, Mapping[str, Any]]:
        if self.dry_run or not urlhashes:
            return {}

        existing: dict[str, Mapping[str, Any]] = {}
        for chunk in self._chunks(sorted(set(urlhashes)), 100):
            data = self._request(
                "GET",
                "articles",
                query={
                    "select": "urlhash,pos_strength,neu_strength,neg_strength,chunk_count",
                    "urlhash": f"in.({','.join(chunk)})",
                },
            )
            for row in data:
                existing[str(row["urlhash"])] = row
        return existing

    def _request(
        self,
        method: str,
        table: str,
        *,
        query: Mapping[str, str] | None = None,
        json_payload: Any | None = None,
        prefer: str | None = None,
    ) -> Any:
        url = f"{self.supabase_url}/rest/v1/{table}"
        headers = {
            "apikey": self.service_role_key or "",
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer

        response = requests.request(
            method,
            url,
            params=dict(query or {}),
            json=json_payload,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase {method} {table} failed: "
                f"{response.status_code} {response.text}"
            )
        if not response.text:
            return []
        return response.json()

    def _strengths_differ(
        self,
        current: Mapping[str, Any],
        stored: Mapping[str, Any],
    ) -> bool:
        pairs = (
            ("pos_strength", "pos_strength"),
            ("neu_strength", "neu_strength"),
            ("neg_strength", "neg_strength"),
        )
        for current_key, stored_key in pairs:
            left = self._optional_float(current.get(current_key))
            right = self._optional_float(stored.get(stored_key))
            if left is None or right is None:
                if left != right:
                    return True
                continue
            if abs(left - right) > self.divergence_epsilon:
                return True
        return False

    @staticmethod
    def _sent_tag(profile: Mapping[str, Any]) -> str:
        if profile.get("state") != "scored":
            return "---"
        positive = float(profile.get("positive_strength") or 0.0)
        neutral = float(profile.get("neutral_strength") or 0.0)
        negative = float(profile.get("negative_strength") or 0.0)
        if positive > neutral and positive > negative:
            return "Pos"
        if negative > neutral and negative > positive:
            return "Neg"
        return "Neu"

    @staticmethod
    def _tag_tally(article_rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        tally = {"Pos": 0, "Neu": 0, "Neg": 0, "---": 0}
        for row in article_rows:
            tag = str(row.get("sent_tag", "---"))
            tally[tag] = tally.get(tag, 0) + 1
        return tally

    @staticmethod
    def _dedupe_by_urlhash(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        seen = set()
        deduped = []
        for row in rows:
            urlhash = row.get("urlhash")
            if urlhash in seen:
                continue
            seen.add(urlhash)
            deduped.append(row)
        return deduped

    @staticmethod
    def _chunk_count(record: Mapping[str, Any], profile: Mapping[str, Any]) -> int | None:
        chunks_used = int(profile.get("chunks_used") or 0)
        chunks_invalid = int(profile.get("chunks_invalid") or 0)
        if chunks_used or chunks_invalid:
            return chunks_used + chunks_invalid
        raw = record.get("chunk_count")
        try:
            return int(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _iso_from_epoch(epoch: float) -> str:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _round_or_none(cls, value: Any, digits: int = 6) -> float | None:
        parsed = cls._optional_float(value)
        if parsed is None:
            return None
        return round(parsed, digits)

    @staticmethod
    def _first_text(record: Mapping[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _chunks(values: Sequence[str], size: int) -> list[Sequence[str]]:
        return [values[index:index + size] for index in range(0, len(values), size)]


def publish_run_to_supabase(**kwargs: Any) -> SupabasePublishResult:
    """Convenience wrapper used by composite_score.py."""

    engine = SupabaseEngine(
        instance_id=kwargs.pop("instance_id", None),
        dry_run=kwargs.pop("dry_run", False),
    )
    return engine.publish_run(**kwargs)
