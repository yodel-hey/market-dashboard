#!/usr/bin/env python3
"""
fetch_market_data.py

Runs hourly via GitHub Actions (cron in UTC). Only does real work when the
current time in America/Chicago (CT) falls inside the configured window
(07:00-15:00 CT), so the timezone/DST handling is correct year-round without
ever touching the cron file. This window is the real-world equivalent of the
user's original 14:00-22:00 Europe/Berlin preference, expressed directly in
CT per their final choice - see the ACTIVE_TZ block below for the accepted
~3-week/year edge case this implies around US/EU DST transition dates.

Data sources (revised 2026-07-20 after the first real Actions run exposed
FMP plan-tier limits - see inline notes at each config block for specifics):
  - FMP            -> only treasury yields and the economic-indicators
                       endpoint (CPI level, inflation expectation, fed funds,
                       retail sales, durable goods, unemployment, nonfarm
                       payroll, initial claims, housing starts, real GDP).
                       batch-quote and economic-calendar both return 402 on
                       this FMP plan - kept in the code (calendar attempt for
                       PMI/GDPNow) since they cost nothing extra and would
                       start working automatically on a higher plan.
  - yfinance        -> ALL prices: stocks/ETFs, cash indices, AND real
                       futures contracts. No API key or plan needed, and
                       proved 100% reliable in the first real run.
  - FRED            -> PPI, average hourly earnings, building permits, PCE
                       (+ core), and GDPNow. All confirmed to exist via
                       FRED's own site search; the API itself was first
                       exercised for real in GitHub Actions.
  - Bigdata.com     -> last-resort fallback ONLY for GDPNow, via the
                       Research Agent (natural-language answer, regex-parsed).
                       Fragile by design - only reached if FMP AND FRED
                       both fail. Logged loudly when it fires.
  - No source found -> ISM/S&P Global PMI. FRED stopped receiving free ISM
                       data years ago; FMP's calendar would have it but is
                       paywalled on this plan. Accepted gap for now.

Every external call is wrapped with retries/backoff so a single rate-limited
or flaky call doesn't kill the whole run - failures are logged into the
output JSON under "_errors" instead.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None  # handled at call time with a clear error

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
BIGDATA_API_KEY = os.environ.get("BIGDATA_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

FMP_BASE = "https://financialmodelingprep.com/stable"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
BIGDATA_AGENTS_BASE = "https://agents.bigdata.com/v1"  # verified against docs.bigdata.com on 2026-07-20

# Active window: only fetch during these local hours in America/Chicago (CT).
# This is the same real-world window as the user's original 14:00-22:00
# Europe/Berlin time - just expressed directly in CT per their preference.
# Known, accepted tradeoff: US and EU daylight-saving transitions don't land
# on the same calendar day (~3 weeks/year mismatch, e.g. mid-March and late
# October 2026). During those weeks this window drifts by 1 hour relative to
# 14:00-22:00 Berlin time. Since the window is now anchored to CT itself
# (not computed as a Berlin-equivalent), this isn't a bug - it's just always
# exactly 07:00-15:00 CT, correctly handled by zoneinfo's own DST rules.
# Cron itself runs hourly in UTC year-round; this check makes the DST
# handling automatic instead of needing the workflow file edited twice a year.
ACTIVE_TIMES = [(0, 8), (2, 9), (5, 8), (8, 9), (9, 10), (12, 9), (15, 11)]  # (CT hour, CT minute) run targets
ACTIVE_TOLERANCE_MINUTES = 20  # allow a run up to this many minutes AFTER a target time (covers GitHub schedule jitter)
ACTIVE_TZ = ZoneInfo("America/Chicago")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data.json")
HISTORY_PATH = os.environ.get("HISTORY_PATH", "history.json")

# All prices/indices/futures via yfinance now (2026-07-20 revision): the
# real first Actions run showed FMP's batch-quote AND economic-calendar both
# return 402 Payment Required on the user's actual FMP plan (my own MCP
# testing during planning used a different, better-provisioned FMP account -
# it proved the endpoints exist, not that this plan can reach them).
# yfinance needs no API key/plan at all and proved 100% reliable in that same
# real run (all 10 futures tickers worked, including ^VVIX and BTC=F which
# were never verifiable from the build sandbox). So: stocks, cash indices,
# and real futures all move to yfinance; FMP is kept only for what actually
# works on this plan (treasury-rates, economic-indicators).
YF_SYMBOLS = [
    # stocks/ETFs
    "SPY", "QQQ", "XLK", "XLF", "XLE",
    # cash indices (same tickers work on Yahoo as they did on FMP)
    "^GSPC", "^NDX", "^DJI", "^N225", "^HSI", "^STOXX50E", "^GDAXI", "^VIX", "^VVIX", "DX-Y.NYB",
    # real futures (Yahoo-only ticker convention, no FMP equivalent)
    "ES=F", "NQ=F", "YM=F", "NKD=F", "6E=F", "ZN=F", "GC=F", "CL=F", "BTC=F",
]

# FRED series added 2026-07-20 to replace the macro indicators that were
# only reachable via FMP's paywalled economic-calendar. All four series IDs
# verified live against fred.stlouisfed.org search (not yet tested against
# the live FRED API itself - same caveat as the GDPNow series below).
FRED_SERIES = {
    "ppi": "PPIFIS",  # Producer Price Index by Commodity: Final Demand
    "average_hourly_earnings": "CES0500000003",  # Average Hourly Earnings of All Employees, Total Private
    "building_permits": "PERMIT",  # New Privately-Owned Housing Units Authorized, Total
    "pce_price_index": "PCEPI",  # PCE Chain-type Price Index
    "core_pce_price_index": "PCEPILFE",  # PCE excluding Food and Energy
}

# FMP economics-indicators: exact valid `name` values per FMP's own docs
# (site.financialmodelingprep.com/developer/docs/stable/economics-indicators)
FMP_INDICATOR_NAMES = {
    "cpi_index": "CPI",
    "inflation_expectation": "inflationRate",  # daily, smooth -> looks like a market-implied/breakeven measure, NOT the monthly CPI print
    "fed_funds_rate": "federalFunds",
    "retail_sales": "retailSales",
    "durable_goods": "durableGoods",
    "unemployment_rate": "unemploymentRate",
    "nonfarm_payroll": "totalNonfarmPayroll",
    "initial_claims": "initialClaims",
    "housing_starts": "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
    "real_gdp": "realGDP",
}

# FMP economics-calendar: trimmed 2026-07-20 to only what has no alternative
# source anywhere else. Everything that used to live here (PPI, CPI YoY, PCE,
# Average Hourly Earnings, Building Permits, Durable Goods) is now covered by
# FRED_SERIES or FMP_INDICATOR_NAMES instead. What's left:
#   - PMI variants: confirmed via live FRED search that ISM no longer
#     distributes this data freely (0 results) - this call is the only
#     remaining attempt, kept in case the user's FMP plan changes.
#   - GDPNow: kept as the primary attempt in the FMP->FRED->Bigdata chain,
#     even though it currently 402s on this plan - costs nothing to keep
#     trying, and it'll start working automatically if the plan is upgraded.
# Currently this whole call 402s on the user's FMP plan, so in practice every
# key below resolves via the "no calendar event found" error path - that's
# expected and harmless, not a bug.
FMP_CALENDAR_EVENTS = {
    "ism_manufacturing_pmi": "ISM Manufacturing PMI",
    "ism_services_pmi": "ISM Services PMI",
    "sp_global_composite_pmi": "S&P Global Composite PMI",
    "sp_global_manufacturing_pmi": "S&P Global Manufacturing PMI",
    "sp_global_services_pmi": "S&P Global Services PMI",
    "gdpnow": "Atlanta Fed GDPNow",
}

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def in_active_window(now_utc=None):
    """True if the current America/Chicago time is at, or up to
    ACTIVE_TOLERANCE_MINUTES after, one of the configured (hour, minute)
    run targets. FORCE_RUN (manual test override via workflow_dispatch
    input) always returns True regardless of the current time."""
    if os.environ.get("FORCE_RUN", "").lower() in ("1", "true", "yes"):
        return True
    now_utc = now_utc or datetime.now(ZoneInfo("UTC"))
    local = now_utc.astimezone(ACTIVE_TZ)
    current_minutes = local.hour * 60 + local.minute
    for target_hour, target_minute in ACTIVE_TIMES:
        target_minutes = target_hour * 60 + target_minute
        if 0 <= current_minutes - target_minutes <= ACTIVE_TOLERANCE_MINUTES:
            return True
    return False


def request_with_retry(method, url, errors_log, label, **kwargs):
    """
    Wraps requests.request with retry/backoff. On final failure, appends a
    note to errors_log and returns None instead of raising - one bad call
    should never kill the whole run.
    """
    last_exc = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.request(method, url, timeout=20, **kwargs)
            if resp.status_code == 429:
                # rate limited - back off and retry
                wait = RETRY_BACKOFF_SECONDS * attempt
                errors_log.append(f"{label}: rate limited (429), waiting {wait}s (attempt {attempt}/{RETRY_ATTEMPTS})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - we deliberately want to catch everything here
            last_exc = exc
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS)
    errors_log.append(f"{label}: failed after {RETRY_ATTEMPTS} attempts - {last_exc}")
    return None


def latest_calendar_actual(events, prefix, errors_log, label):
    """
    Given the raw economics-calendar event list, find the most recent event
    whose 'event' field starts with `prefix` and return its actual value
    (falling back to 'previous' if actual hasn't posted yet for the latest period).
    """
    matches = [e for e in events if isinstance(e.get("event"), str) and e["event"].startswith(prefix)]
    if not matches:
        errors_log.append(f"{label}: no calendar event found starting with '{prefix}'")
        return None
    matches.sort(key=lambda e: e.get("date", ""), reverse=True)
    top = matches[0]
    value = top.get("actual")
    if value is None:
        value = top.get("previous")
    return {"value": value, "date": top.get("date"), "event": top.get("event")}


# ---------------------------------------------------------------------------
# FMP fetchers
# ---------------------------------------------------------------------------

def fmp_get(path, params, errors_log, label):
    params = dict(params or {})
    params["apikey"] = FMP_API_KEY
    return request_with_retry("GET", f"{FMP_BASE}/{path}", errors_log, label, params=params)


def fetch_fmp_treasury_rates(errors_log):
    today = date.today()
    week_ago = today - timedelta(days=7)
    data = fmp_get(
        "treasury-rates",
        {"from": week_ago.isoformat(), "to": today.isoformat()},
        errors_log,
        "fmp_treasury_rates",
    )
    if not data:
        return {}
    # most recent entry first
    data.sort(key=lambda r: r["date"], reverse=True)
    return data[0]


QUARTERLY_INDICATOR_NAMES = {"realGDP", "GDP", "nominalPotentialGDP", "realGDPPerCapita"}


def fetch_fmp_indicators(errors_log):
    """
    NOTE (revised 2026-07-20, second pass): the first fix removed from/to
    entirely to solve the GDP gap, but the very next real run showed that
    was a regression - without explicit dates, this endpoint returned
    noticeably OLDER data across the board on this FMP plan (e.g. CPI dated
    2025-09 instead of 2026-06 from the prior run). Likely another case of
    this plan behaving differently than the one used during planning.
    Restored explicit from/to (matches what worked in the very first real
    run) for monthly/weekly series. For quarterly series (GDP and friends),
    a single 90-day window can legitimately miss the latest release since
    FMP reports the *period start* date, not the release date - so those
    specifically step back through additional 90-day windows until data is
    found (up to ~14 months back, comfortably covering a missed quarter).
    """
    today = date.today()
    results = {}
    for key, name in FMP_INDICATOR_NAMES.items():
        data = None
        window_end = today
        max_windows = 5 if name in QUARTERLY_INDICATOR_NAMES else 1
        for _ in range(max_windows):
            window_start = window_end - timedelta(days=89)
            data = fmp_get(
                "economic-indicators",
                {"name": name, "from": window_start.isoformat(), "to": window_end.isoformat()},
                errors_log,
                f"fmp_indicator_{key}",
            )
            if data:
                break
            window_end = window_start  # step back another 90 days and retry
        if not data:
            results[key] = None
            continue
        data.sort(key=lambda r: r["date"], reverse=True)
        results[key] = {"value": data[0]["value"], "date": data[0]["date"]}
    return results


def fetch_fmp_calendar_events(errors_log):
    today = date.today()
    sixty_days_ago = today - timedelta(days=60)
    events = fmp_get(
        "economic-calendar",
        {"from": sixty_days_ago.isoformat(), "to": today.isoformat()},
        errors_log,
        "fmp_calendar",
    )
    if not events:
        return {}
    us_events = [e for e in events if e.get("country") == "US"]
    results = {}
    for key, prefix in FMP_CALENDAR_EVENTS.items():
        results[key] = latest_calendar_actual(us_events, prefix, errors_log, f"fmp_calendar_{key}")
    return results


# ---------------------------------------------------------------------------
# yfinance fetcher
# ---------------------------------------------------------------------------

def fetch_yfinance_symbols(errors_log):
    if yf is None:
        errors_log.append("yfinance: package not installed")
        return {}
    results = {}
    for symbol in YF_SYMBOLS:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2d")
            if hist.empty:
                errors_log.append(f"yfinance_{symbol}: no data returned")
                results[symbol] = None
                continue
            last_close = float(hist["Close"].iloc[-1])
            prev_close = float(hist["Close"].iloc[-2]) if len(hist) > 1 else None
            change_pct = ((last_close - prev_close) / prev_close * 100) if prev_close else None
            results[symbol] = {"price": last_close, "changePercent": change_pct}
        except Exception as exc:  # noqa: BLE001
            errors_log.append(f"yfinance_{symbol}: {exc}")
            results[symbol] = None
    return results


# ---------------------------------------------------------------------------
# GDPNow: FMP primary -> FRED fallback -> Bigdata Research Agent last resort
# ---------------------------------------------------------------------------

def fetch_fred_series(series_id, errors_log, label):
    """
    Generic single-series fetch against FRED's observations endpoint.
    Returns the most recent observation as {"value": float, "date": str}, or
    None on any failure (missing key, network error, or unparseable response).
    NOTE: FRED has never been reachable from the build sandbox (no network
    route there) - every series here was verified to exist via FRED's own
    website search, but the API call itself was first tested for real in
    GitHub Actions, not by me during planning.
    """
    if not FRED_API_KEY:
        errors_log.append(f"{label}: FRED_API_KEY not set, skipping")
        return None
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
    }
    data = request_with_retry("GET", FRED_BASE, errors_log, label, params=params)
    if not data:
        return None
    try:
        obs = data["observations"][0]
        return {"value": float(obs["value"]), "date": obs["date"]}
    except (KeyError, IndexError, ValueError) as exc:
        errors_log.append(f"{label}: unexpected response shape - {exc}")
        return None


def fetch_fred_indicators(errors_log):
    """Fetches all of FRED_SERIES (the PPI/earnings/permits/PCE additions)."""
    return {key: fetch_fred_series(series_id, errors_log, f"fred_{key}") for key, series_id in FRED_SERIES.items()}


def fetch_gdpnow_fred(errors_log):
    result = fetch_fred_series("GDPNOW", errors_log, "fred_gdpnow")
    if result:
        result["source"] = "FRED"
    return result


def fetch_bigdata_research_value(query, label, errors_log):
    """
    Generic last-resort fallback: asks Bigdata's Research Agent a plain-
    language question and requests a structured numeric answer directly via
    structured_output_schema, rather than blindly regexing free text.
    Still fragile by design - a research-agent answer, not structured market
    data - so it's logged loudly whenever it actually fires.

    IMPORTANT: this endpoint returns a Server-Sent Events (SSE) stream, not a
    single JSON blob - confirmed against Bigdata's own live API docs on
    2026-07-20. An earlier draft of this function assumed a plain JSON
    response and a completely different URL/field names - both wrong, and
    never actually reachable from the build sandbox to test directly (same
    limitation as FRED/yfinance during planning). This is a best-effort
    implementation against the documented schema; the first real run in
    GitHub Actions is the first real test of the SSE-parsing logic itself.
    """
    if not BIGDATA_API_KEY:
        errors_log.append(f"{label}: BIGDATA_API_KEY not set, skipping")
        return None
    errors_log.append(f"{label}: FALLBACK IN USE - value comes from a research agent, not structured market data")
    headers = {"X-API-Key": BIGDATA_API_KEY, "Content-Type": "application/json", "Accept": "text/event-stream"}
    payload = {
        "message": query,
        "research_effort": "lite",
        "structured_output_schema": {
            "type": "object",
            "properties": {"value": {"type": "number"}},
            "required": ["value"],
        },
    }
    try:
        resp = requests.post(
            f"{BIGDATA_AGENTS_BASE}/research-agent", headers=headers, json=payload, timeout=120, stream=True
        )
        resp.raise_for_status()
        structured_value = None
        fallback_value = None
        raw_lines_seen = []
        message_types_seen = []
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            raw_lines_seen.append(line)
            if not line.startswith("data:"):
                continue
            try:
                event = json.loads(line[len("data:"):].strip())
            except json.JSONDecodeError:
                continue
            message = event.get("message", {})
            message_type = message.get("type")
            message_types_seen.append(message_type)
            content = message.get("content")
            # Fixed 2026-07-20 after seeing real responses: actual type strings
            # are "STRUCTURED_OUTPUT"/"ANSWER"/"COMPLETE" (not the CamelCase
            # names implied by the docs), and STRUCTURED_OUTPUT's content
            # arrives as an actual dict already, not a JSON string to parse.
            # STRUCTURED_OUTPUT is preferred when present; ANSWER/COMPLETE
            # free text is only used if no structured value ever showed up.
            if message_type == "STRUCTURED_OUTPUT":
                if isinstance(content, dict) and "value" in content:
                    structured_value = content["value"]
            elif message_type in ("ANSWER", "COMPLETE"):
                if isinstance(content, str):
                    match = re.search(r"(-?\d+\.?\d*)", content)
                    if match:
                        fallback_value = float(match.group(1))
        final_value = structured_value if structured_value is not None else fallback_value
        if final_value is None:
            tail_preview = " | ".join(raw_lines_seen[-3:])[:800] if raw_lines_seen else "(no lines received at all)"
            errors_log.append(
                f"{label}: no usable value found - {len(raw_lines_seen)} lines total, "
                f"types seen: {message_types_seen} - tail: {tail_preview}"
            )
            return None
        return {"value": float(final_value), "source": "Bigdata Research Agent (fallback, not structured market data)"}
    except Exception as exc:  # noqa: BLE001
        errors_log.append(f"{label}: request failed - {exc}")
        return None


def fetch_gdpnow_bigdata(errors_log):
    return fetch_bigdata_research_value(
        "What is the most recent Atlanta Fed GDPNow estimate, as a percentage number?",
        "bigdata_gdpnow",
        errors_log,
    )


def fetch_gdpnow(fmp_calendar_results, errors_log):
    primary = fmp_calendar_results.get("gdpnow")
    if primary and primary.get("value") is not None:
        return {"value": primary["value"], "date": primary.get("date"), "source": "FMP"}
    errors_log.append("gdpnow: FMP calendar had no value, falling back to FRED")
    fred_result = fetch_gdpnow_fred(errors_log)
    if fred_result:
        return fred_result
    errors_log.append("gdpnow: FRED also failed, falling back to Bigdata Research Agent (last resort)")
    return fetch_gdpnow_bigdata(errors_log)


# ---------------------------------------------------------------------------
# PMI: FMP calendar (paywalled on this plan) -> Bigdata Research Agent fallback
# No FRED option exists here (ISM stopped free distribution to FRED years
# ago, confirmed via a live FRED search returning 0 results). So this is a
# two-level chain, not three like GDPNow - straight from FMP to the fragile
# text-extraction fallback since there's nothing structured in between.
# PMI values are typically plain numbers like "52.3", not percentages, so a
# different regex than GDPNow's is used - still fragile, still logged loudly.
# ---------------------------------------------------------------------------

BIGDATA_PMI_QUERIES = {
    "ism_manufacturing_pmi": "What is the most recent ISM Manufacturing PMI value, as a plain number?",
    "ism_services_pmi": "What is the most recent ISM Services PMI value, as a plain number?",
    "sp_global_composite_pmi": "What is the most recent S&P Global US Composite PMI value, as a plain number?",
    "sp_global_manufacturing_pmi": "What is the most recent S&P Global US Manufacturing PMI value, as a plain number?",
    "sp_global_services_pmi": "What is the most recent S&P Global US Services PMI value, as a plain number?",
}


def fetch_pmi_indicators(fmp_calendar_results, errors_log):
    results = {}
    for key, query in BIGDATA_PMI_QUERIES.items():
        primary = fmp_calendar_results.get(key)
        if primary and primary.get("value") is not None:
            results[key] = {"value": primary["value"], "date": primary.get("date"), "source": "FMP"}
            continue
        errors_log.append(f"{key}: FMP calendar had no value, falling back to Bigdata Research Agent")
        results[key] = fetch_bigdata_research_value(query, f"bigdata_{key}", errors_log)
    return results


# ---------------------------------------------------------------------------
# Calculations (no API calls)
# ---------------------------------------------------------------------------

def compute_spread_bps(treasury_rates):
    try:
        return round((treasury_rates["year10"] - treasury_rates["year2"]) * 100)
    except (KeyError, TypeError):
        return None


def compute_real_rate(treasury_rates, inflation_expectation):
    """
    Real 10Y rate = nominal 10Y yield - inflation expectation.
    NOTE (documented assumption from planning): `inflationRate` from FMP's
    economics-indicators updates daily with smooth ~2-3% values, which looks
    like a market-implied/breakeven measure rather than the realized monthly
    CPI print. This gives a more theoretically correct real yield than
    subtracting the lagging CPI YoY print - but this has not been independently
    confirmed against FMP's own documentation of what "inflationRate" represents.
    """
    try:
        nominal = treasury_rates["year10"]
        expectation = inflation_expectation["value"]
        return round(nominal - expectation, 2)
    except (KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    errors_log = []
    now_utc = datetime.now(ZoneInfo("UTC"))

    if os.environ.get("FORCE_RUN", "").lower() in ("1", "true", "yes"):
        print("FORCE_RUN is set - running regardless of the configured active hours (test override).")

    if not in_active_window(now_utc):
        local_time = now_utc.astimezone(ACTIVE_TZ)
        print(f"Outside active window ({local_time.strftime('%H:%M %Z')}), skipping this run.")
        return

    if not FMP_API_KEY:
        print("FMP_API_KEY is not set - cannot continue.", file=sys.stderr)
        sys.exit(1)

    prices = fetch_yfinance_symbols(errors_log)
    treasury_rates = fetch_fmp_treasury_rates(errors_log)
    indicators = fetch_fmp_indicators(errors_log)
    fred_indicators = fetch_fred_indicators(errors_log)
    calendar_events = fetch_fmp_calendar_events(errors_log)
    gdpnow = fetch_gdpnow(calendar_events, errors_log)
    pmi = fetch_pmi_indicators(calendar_events, errors_log)

    output = {
        "generated_at": now_utc.isoformat(),
        "prices": prices,
        "treasury_rates": treasury_rates,
        "indicators": indicators,
        "fred_indicators": fred_indicators,
        "pmi": pmi,
        "gdpnow": gdpnow,
        "calculated": {
            "spread_2s10s_bps": compute_spread_bps(treasury_rates),
            "real_10y_rate": compute_real_rate(treasury_rates, indicators.get("inflation_expectation")),
        },
        "_errors": errors_log,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    # Append this run's snapshot to history.json for the dashboard's charts.
    # Unbounded by design (user decision 2026-07-21): every field from
    # `output` except _errors is kept, no pruning/rolling window.
    history_entry = {
        "timestamp": output["generated_at"],
        "prices": prices,
        "treasury_rates": treasury_rates,
        "indicators": indicators,
        "fred_indicators": fred_indicators,
        "pmi": pmi,
        "gdpnow": gdpnow,
        "calculated": output["calculated"],
    }

    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH, "r") as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = []
    else:
        history = []

    history.append(history_entry)

    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2, default=str)

    print(f"Wrote {OUTPUT_PATH} with {len(errors_log)} logged issue(s).")
    print(f"Appended snapshot to {HISTORY_PATH} ({len(history)} entries total).")
    if errors_log:
        for e in errors_log:
            print(f"  - {e}")


if __name__ == "__main__":
    main()
