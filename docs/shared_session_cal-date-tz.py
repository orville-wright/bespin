"""
session.py -- shared trading-session resolution for Bespin screeners.

BOTH collectors must import this. Two implementations will drift, and the
failure is silent: rows land in different target_session buckets and the
pooled table quietly stops pooling.

Semantics (forward-looking): target_session is the next US equity session
you could still act on -- the first session whose close is still ahead of
the current instant.

    run 08:00 ET Mon (pre-market)  -> Mon   (Mon close 16:00 still ahead)
    run 17:00 ET Mon (post-close)  -> Tue   (Mon already closed)
    run 10:00 ET Sat               -> Mon   (no weekend session)
    run 11:00 ET Thanksgiving      -> Fri   (holiday, no session)

Because the rule keys off an instant rather than a local date, Auckland and
San Francisco land on the same session automatically:

    16:00 ET Mon == 08:00 Tue Auckland == 13:00 Mon San Francisco

All three are the same moment, and all three resolve to Tuesday's session.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# Longest observed US market closure is well under this. 10 calendar days
# gives comfortable margin for holiday clusters and weather closures.
LOOKAHEAD_DAYS = 10

_cache: dict[tuple[date, date], list] = {}


class SessionResolutionError(RuntimeError):
    """Raised when the target session cannot be determined."""


# ------------------------------------------------------------------
# Pure logic -- no network, unit-testable
# ------------------------------------------------------------------
def resolve_from_calendar(sessions, now_utc: datetime) -> date:
    """
    Pick the first session whose close is still in the future.

    Args:
        sessions: iterable of objects with `.date` (datetime.date) and
                  `.close` (NAIVE datetime in ET wall-clock, as returned
                  by alpaca-py's Calendar model). Must be ascending.
        now_utc:  timezone-aware UTC instant.

    Returns:
        datetime.date of the target session.
    """
    if now_utc.tzinfo is None:
        raise SessionResolutionError(
            "now_utc must be timezone-aware; got a naive datetime. "
            "Use datetime.now(timezone.utc), never datetime.now()."
        )

    now_et = now_utc.astimezone(ET)

    for s in sessions:
        # alpaca-py hands back naive ET wall-clock. Attach the zone rather
        # than converting -- the value is already ET, it just lacks tzinfo.
        close_et = s.close.replace(tzinfo=ET)
        if close_et > now_et:
            return s.date

    raise SessionResolutionError(
        f"No session with a close after {now_et.isoformat()} in the supplied "
        f"calendar window. Widen LOOKAHEAD_DAYS."
    )


# ------------------------------------------------------------------
# Network
# ------------------------------------------------------------------
def fetch_calendar(client, start: date, end: date, use_cache: bool = True):
    """Fetch the Alpaca market calendar for [start, end]. Cached per range."""
    from alpaca.trading.requests import GetCalendarRequest

    key = (start, end)
    if use_cache and key in _cache:
        return _cache[key]

    sessions = client.get_calendar(GetCalendarRequest(start=start, end=end))
    sessions = sorted(sessions, key=lambda s: s.date)

    if use_cache:
        _cache[key] = sessions
    return sessions


def get_target_session(client, now_utc: datetime | None = None) -> date:
    """
    Resolve the forward-looking target session via the Alpaca calendar.

    Fails loudly rather than guessing. A wrong session date silently
    corrupts the shared table; a failed run just needs re-running.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    now_et_date = now_utc.astimezone(ET).date()

    # Start one day back so a run just after midnight ET still sees the
    # session that is technically "yesterday" but has not closed.
    start = now_et_date - timedelta(days=1)
    end = now_et_date + timedelta(days=LOOKAHEAD_DAYS)

    try:
        sessions = fetch_calendar(client, start, end)
    except Exception as e:
        raise SessionResolutionError(
            f"Could not reach the Alpaca calendar API: {e}. "
            f"Refusing to guess the session date."
        ) from e

    if not sessions:
        raise SessionResolutionError(
            f"Alpaca returned an empty calendar for {start}..{end}."
        )

    return resolve_from_calendar(sessions, now_utc)


# ------------------------------------------------------------------
def describe(client, now_utc: datetime | None = None) -> str:
    """Human-readable explanation. Useful in logs and when debugging drift."""
    now_utc = now_utc or datetime.now(timezone.utc)
    sess = get_target_session(client, now_utc)
    now_et = now_utc.astimezone(ET)
    return (
        f"now_utc={now_utc.isoformat()} "
        f"| now_et={now_et.strftime('%Y-%m-%d %H:%M %Z')} "
        f"| target_session={sess.isoformat()}"
    )