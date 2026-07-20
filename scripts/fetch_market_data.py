#!/usr/bin/env python3
"""
fetch_market_data.py

Runs hourly via GitHub Actions (cron in UTC). Only does real work when the
current time in America/Chicago falls inside the user's configured window
(14:00-22:00 CT), so the timezone/DST handling is correct year-round without
ever touching the cron file.

Data sources (all decided and verified live during planning, 2026-07-20):
  - FMP            -> prices, indices, commodities, treasury yields,
                       clean economic indicators, and the economic calendar
                       (for PPI/PMI/GDPNow, which have no clean indicator name).
  - yfinance        -> real futures contracts (ES=F, NQ=F, ...) that only
                       exist under Yahoo's own ticker convention.
  - FRED            -> fallback ONLY for GDPNow, if FMP's calendar doesn't
                       have it for the current quarter yet. NEVER live-tested
                       from the build sandbox (no network route there) -
                       first real run in Actions is the first real test.
  - Bigdata.com     -> last-resort fallback ONLY for GDPNow, via the
                       Research Agent (natural-language answer, regex-parsed).
                       Fragile by design - only reached if FMP AND FRED
                       both fail. Logged loudly when it fires.

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
BIGDATA_RESEARCH_BASE = "https://api.bigdata.com/v1"  # research/chat endpoint - see fetch_gdpnow_bigdata()

# Active window: only fetch during these local hours (America/Chicago).
# Cron itself runs hourly in UTC year-round; this check makes the DST
# handling automatic instead of needing the workflow file edited twice a year.
ACTIVE_HOUR_START = 14
ACTIVE_HOUR_END = 22  # inclusive
ACTIVE_TZ = ZoneInfo("America/Chicago")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data.json")

# Symbols, per the final table worked out during planning.
# GCUSD folds into the same batch-quote call as everything else - the
# generic /quote endpoint was verified live to handle commodities fine,
# so there's no need for the separate (and untested) batch-commodity-quotes
# endpoint with its uncertain parameter support.
FMP_PRICE_SYMBOLS = ["SPY", "QQQ", "XLK", "XLF", "XLE", "GCUSD"]
FMP_INDEX_SYMBOLS = ["^GSPC", "^NDX", "^DJI", "^N225", "^HSI", "^STOXX50E", "^GDAXI", "^VIX", "DX-Y.NYB"]

YF_FUTURES_SYMBOLS = ["ES=F", "NQ=F", "YM=F", "NKD=F", "6E=F", "ZN=F", "GC=F", "CL=F", "BTC=F", "^VVIX"]

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

# FMP economics-calendar: these have NO clean indicator name, only appear as
# calendar events. Matched by the event name's prefix (before " (Mon)").
# Verified live against a real calendar pull on 2026-07-20.
FMP_CALENDAR_EVENTS = {
    "ppi_yoy": "Producer Price Index (YoY)",
    "ppi_mom": "Producer Price Index (MoM)",
    "core_ppi_yoy": "Producer Price Index ex Food, Energy and Trade YoY",
    "cpi_yoy": "Consumer Price Index (YoY)",
    "core_cpi_yoy": "Consumer Price Index ex Food & Energy (YoY)",
    "pce_yoy": "Personal Consumption Expenditures - Price Index (YoY)",
    "core_pce_yoy": "Core Personal Consumption Expenditures - Price Index (YoY)",
    "ism_manufacturing_pmi": "ISM Manufacturing PMI",
    "ism_services_pmi": "ISM Services PMI",
    "sp_global_composite_pmi": "S&P Global Composite PMI",
    "sp_global_manufacturing_pmi": "S&P Global Manufacturing PMI",
    "sp_global_services_pmi": "S&P Global Services PMI",
    "average_hourly_earnings_mom": "Average Hourly Earnings (MoM)",
    "average_hourly_earnings_yoy": "Average Hourly Earnings (YoY)",
    "building_permits": "Building Permits",
    "durable_goods_orders": "Durable Goods Orders",
    "gdpnow": "Atlanta Fed GDPNow",
}

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def in_active_window(now_utc=None):
    """True if the current America/Chicago time is inside the configured window."""
    now_utc = now_utc or datetime.now(ZoneInfo("UTC"))
    local = now_utc.astimezone(ACTIVE_TZ)
    return ACTIVE_HOUR_START <= local.hour <= ACTIVE_HOUR_END


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


def fetch_fmp_prices(errors_log):
    symbols = ",".join(FMP_PRICE_SYMBOLS + FMP_INDEX_SYMBOLS)
    data = fmp_get("batch-quote", {"symbols": symbols}, errors_log, "fmp_prices")
    if not data:
        return {}
    return {row["symbol"]: {"price": row.get("price"), "changePercent": row.get("changePercentage")} for row in data}


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


def fetch_fmp_indicators(errors_log):
    today = date.today()
    ninety_days_ago = today - timedelta(days=89)  # FMP caps this endpoint at a 90-day range
    results = {}
    for key, name in FMP_INDICATOR_NAMES.items():
        data = fmp_get(
            "economic-indicators",
            {"name": name, "from": ninety_days_ago.isoformat(), "to": today.isoformat()},
            errors_log,
            f"fmp_indicator_{key}",
        )
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

def fetch_yfinance_futures(errors_log):
    if yf is None:
        errors_log.append("yfinance: package not installed")
        return {}
    results = {}
    for symbol in YF_FUTURES_SYMBOLS:
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

def fetch_gdpnow_fred(errors_log):
    if not FRED_API_KEY:
        errors_log.append("fred_gdpnow: FRED_API_KEY not set, skipping")
        return None
    params = {
        "series_id": "GDPNOW",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
    }
    data = request_with_retry("GET", FRED_BASE, errors_log, "fred_gdpnow", params=params)
    if not data:
        return None
    try:
        obs = data["observations"][0]
        return {"value": float(obs["value"]), "date": obs["date"], "source": "FRED"}
    except (KeyError, IndexError, ValueError) as exc:
        errors_log.append(f"fred_gdpnow: unexpected response shape - {exc}")
        return None


def fetch_gdpnow_bigdata(errors_log):
    """
    Last-resort fallback. Asks Bigdata's Research Agent a plain-language
    question and regex-extracts a percentage. Deliberately noisy in the logs
    when it fires, since this path is fragile by design (see planning notes).
    """
    if not BIGDATA_API_KEY:
        errors_log.append("bigdata_gdpnow: BIGDATA_API_KEY not set, skipping")
        return None
    errors_log.append("bigdata_gdpnow: FALLBACK LEVEL 3 IN USE - value is text-extracted and unverified")
    headers = {"X-API-KEY": BIGDATA_API_KEY, "Content-Type": "application/json"}
    payload = {"query": "What is the most recent Atlanta Fed GDPNow estimate, as a percentage?"}
    data = request_with_retry(
        "POST", f"{BIGDATA_RESEARCH_BASE}/research/query", errors_log, "bigdata_gdpnow", headers=headers, json=payload
    )
    if not data:
        return None
    text = json.dumps(data)
    match = re.search(r"(-?\d+\.?\d*)\s?%", text)
    if not match:
        errors_log.append("bigdata_gdpnow: could not find a percentage in the response text")
        return None
    return {"value": float(match.group(1)), "source": "Bigdata Research Agent (text-extracted, unverified)"}


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

    if not in_active_window(now_utc):
        local_time = now_utc.astimezone(ACTIVE_TZ)
        print(f"Outside active window ({local_time.strftime('%H:%M %Z')}), skipping this run.")
        return

    if not FMP_API_KEY:
        print("FMP_API_KEY is not set - cannot continue.", file=sys.stderr)
        sys.exit(1)

    prices = fetch_fmp_prices(errors_log)
    treasury_rates = fetch_fmp_treasury_rates(errors_log)
    indicators = fetch_fmp_indicators(errors_log)
    calendar_events = fetch_fmp_calendar_events(errors_log)
    futures = fetch_yfinance_futures(errors_log)
    gdpnow = fetch_gdpnow(calendar_events, errors_log)

    output = {
        "generated_at": now_utc.isoformat(),
        "prices": prices,
        "futures": futures,
        "treasury_rates": treasury_rates,
        "indicators": indicators,
        "calendar_events": {k: v for k, v in calendar_events.items() if k != "gdpnow"},
        "gdpnow": gdpnow,
        "calculated": {
            "spread_2s10s_bps": compute_spread_bps(treasury_rates),
            "real_10y_rate": compute_real_rate(treasury_rates, indicators.get("inflation_expectation")),
        },
        "_errors": errors_log,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Wrote {OUTPUT_PATH} with {len(errors_log)} logged issue(s).")
    if errors_log:
        for e in errors_log:
            print(f"  - {e}")


if __name__ == "__main__":
    main()
