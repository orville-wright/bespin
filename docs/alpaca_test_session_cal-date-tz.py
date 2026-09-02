"""
test_session.py -- exercises session.resolve_from_calendar with a synthetic
Alpaca-shaped calendar. No network.

Run: python test_session.py
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from session import ET, resolve_from_calendar, SessionResolutionError

AKL = ZoneInfo("Pacific/Auckland")
SFO = ZoneInfo("America/Los_Angeles")


@dataclass
class FakeCalendar:
    """Mimics alpaca-py's Calendar: naive ET wall-clock open/close."""
    date: date
    open: datetime
    close: datetime


def session(d: date, close_hhmm=(16, 0)) -> FakeCalendar:
    return FakeCalendar(
        date=d,
        open=datetime(d.year, d.month, d.day, 9, 30),
        close=datetime(d.year, d.month, d.day, *close_hhmm),
    )


# Nov/Dec 2026 window covering a normal week, Thanksgiving (Thu 26 Nov 2026),
# the half-day Friday after, and the following week.
CAL = [
    session(date(2026, 11, 23)),                    # Mon
    session(date(2026, 11, 24)),                    # Tue
    session(date(2026, 11, 25)),                    # Wed
    # Thu 26 Nov = Thanksgiving, no session
    session(date(2026, 11, 27), (13, 0)),           # Fri, half day
    session(date(2026, 11, 30)),                    # Mon
    session(date(2026, 12, 1)),                     # Tue
]

results = []


def check(name, expected, actual):
    results.append((name, str(expected), str(actual), "PASS" if expected == actual else "*** FAIL ***"))


def et(y, m, d, hh, mm):
    """Build a UTC instant from an ET wall-clock time."""
    return datetime(y, m, d, hh, mm, tzinfo=ET).astimezone(timezone.utc)


# ---- core forward-looking semantics ------------------------------
check("S01 pre-market Mon -> Mon",
      date(2026, 11, 23), resolve_from_calendar(CAL, et(2026, 11, 23, 8, 0)))

check("S02 mid-session Mon -> Mon",
      date(2026, 11, 23), resolve_from_calendar(CAL, et(2026, 11, 23, 11, 0)))

check("S03 one minute before close -> Mon",
      date(2026, 11, 23), resolve_from_calendar(CAL, et(2026, 11, 23, 15, 59)))

check("S04 one minute after close -> Tue",
      date(2026, 11, 24), resolve_from_calendar(CAL, et(2026, 11, 23, 16, 1)))

check("S05 late evening Mon -> Tue",
      date(2026, 11, 24), resolve_from_calendar(CAL, et(2026, 11, 23, 23, 30)))

# ---- holiday and half-day ----------------------------------------
check("S06 Wed after close -> Fri (Thanksgiving skipped)",
      date(2026, 11, 27), resolve_from_calendar(CAL, et(2026, 11, 25, 17, 0)))

check("S07 Thanksgiving morning -> Fri",
      date(2026, 11, 27), resolve_from_calendar(CAL, et(2026, 11, 26, 10, 0)))

check("S08 half-day Fri 12:59 -> Fri",
      date(2026, 11, 27), resolve_from_calendar(CAL, et(2026, 11, 27, 12, 59)))

check("S09 half-day Fri 13:01 -> Mon (early close honoured)",
      date(2026, 11, 30), resolve_from_calendar(CAL, et(2026, 11, 27, 13, 1)))

# ---- weekend ------------------------------------------------------
check("S10 Saturday -> Mon",
      date(2026, 11, 30), resolve_from_calendar(CAL, et(2026, 11, 28, 10, 0)))

check("S11 Sunday night -> Mon",
      date(2026, 11, 30), resolve_from_calendar(CAL, et(2026, 11, 29, 22, 0)))

# ---- THE POINT: Auckland and San Francisco converge ---------------
# 16:00 ET Mon 23 Nov is the same instant as 10:00 Tue 24 Nov in Auckland
# and 13:00 Mon 23 Nov in San Francisco. Both must resolve to Tuesday.
akl_run = datetime(2026, 11, 24, 11, 0, tzinfo=AKL).astimezone(timezone.utc)
sfo_run = datetime(2026, 11, 23, 14, 0, tzinfo=SFO).astimezone(timezone.utc)

akl_target = resolve_from_calendar(CAL, akl_run)
sfo_target = resolve_from_calendar(CAL, sfo_run)

check("S12 Auckland 11:00 Tue -> Tue session",
      date(2026, 11, 24), akl_target)
check("S13 San Francisco 14:00 Mon -> Tue session",
      date(2026, 11, 24), sfo_target)
check("S14 collectors agree", True, akl_target == sfo_target)

# Same window, opposite end: both just before the Tuesday close.
akl_run2 = datetime(2026, 11, 25, 7, 0, tzinfo=AKL).astimezone(timezone.utc)
sfo_run2 = datetime(2026, 11, 24, 10, 0, tzinfo=SFO).astimezone(timezone.utc)
check("S15 collectors agree near window end", True,
      resolve_from_calendar(CAL, akl_run2) == resolve_from_calendar(CAL, sfo_run2))

# ---- guardrails ---------------------------------------------------
try:
    resolve_from_calendar(CAL, datetime(2026, 11, 23, 8, 0))   # naive
    check("S16 naive now_utc rejected", "raise", "accepted")
except SessionResolutionError:
    check("S16 naive now_utc rejected", "raise", "raise")

try:
    resolve_from_calendar(CAL, et(2027, 1, 1, 10, 0))          # past window
    check("S17 exhausted window raises", "raise", "accepted")
except SessionResolutionError:
    check("S17 exhausted window raises", "raise", "raise")

# ---- report -------------------------------------------------------
print("\n" + "=" * 74)
print(f"{'CHECK':<44}{'EXPECTED':<14}{'ACTUAL':<12}{'STATUS'}")
print("=" * 74)
for n, e, a, s in results:
    print(f"{n:<44}{e[:13]:<14}{a[:11]:<12}{s}")
print("=" * 74)
nf = sum(1 for r in results if r[3] != "PASS")
print(f"{len(results) - nf}/{len(results)} passed" + ("" if nf == 0 else f"  --  {nf} FAILED"))

print("\n--- convergence detail ---")
for label, dt in [("Auckland", akl_run), ("San Francisco", sfo_run)]:
    print(f"{label:<16} local={dt.astimezone(AKL if label=='Auckland' else SFO)}"
          f"  ET={dt.astimezone(ET).strftime('%Y-%m-%d %H:%M %Z')}"
          f"  -> {resolve_from_calendar(CAL, dt)}")