"""
Hyblock Strategy Backtest
=========================
Combines 1H OHLCV (CryptoCompare) with Hyblock historical signals to
backtest the full trading strategy over the last ~41 days.

Hyblock signals used (all 1H bars, up to 1000 candles):
  - bidAsk          -> OBI = (bid-ask)/(bid+ask)
  - volumeDelta     -> buy vs sell flow
  - whaleRetailDelta-> whale sentiment
  - fundingRate     -> contrarian funding signal
  - topTraderPositions -> top trader L/S ratio (contrarian)
  - globalAccounts  -> overall market positioning
  - openInterest    -> OI trend / rate of change

Strategy filters replicated:
  1. 1H + 6H Heikin Ashi alignment
  2. $1k zone system with 2h cooldown + max 2 entries per zone
  3. Second-break (dwarf) rule
  4. 2H velocity filter (block if momentum is AGAINST trade >1.5%)
  5. Funding rate contrarian filter (from Hyblock)
  6. Hyblock confidence score (OBI, whale, top traders, vol delta, OI trend)
  7. Confidence minimum gate
  8. Trailing stop: TP1=20% activates 1% trail; TP2=30% loosens to 5%
  9. Emergency close: 4 consecutive opposing 1H HA candles

Run:
    python backtest_hyblock.py
"""

import asyncio
import base64
import json
import time
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

# -- Credentials ---------------------------------------------------------------
HYBLOCK_API_KEY   = "e5ijeNidDE5C8T5k5o9Ux5wEvOWMgCRS8TiMeWF5"
HYBLOCK_CLIENT_ID = "b27429a6-4e38-4a49-9440-6f0e16cdc3f1"
HYBLOCK_SECRET    = "x4j_VaXJT1EcuHG3dzj3BpZyhMXLIj3_LKkhrZ_yT8MRd5BsqJXv2mA-n2tyBqGV"
HYBLOCK_BASE      = "https://api.hyblockcapital.com/v2"
EXCHANGE          = "binance_perp_stable"
CC_BASE           = "https://min-api.cryptocompare.com"

# -- Strategy params -----------------------------------------------------------
LEVERAGE         = 75
TP1_PCT          = 20.0      # peak% to activate trail
TP2_PCT          = 30.0      # peak% for wide trail
TRAIL_AFTER_TP1  = 1.0       # drawdown% from peak -> exit after TP1
TRAIL_AFTER_TP2  = 5.0       # drawdown% from peak -> exit after TP2
ZONE_SIZE        = 1000.0
ZONE_COOLDOWN_H  = 2
MAX_ZONE_ENTRIES = 2
VELOCITY_WIN     = 2         # bars (1H each)
VELOCITY_MIN     = 1.5       # % move
EMERGENCY_BARS   = 4         # consecutive opposing HA bars -> close
CONFIDENCE_MIN   = 50.0      # minimum confidence to enter (50 = no extra filter)
TIMEFRAME        = "4h"      # Hyblock timeframe: "1h"=41d, "4h"=83d, "1d"=2yr+
LIMIT            = 500       # bars of historical data to fetch (4h*500 = ~83 days)
# CryptoCompare histohour max = 2000; 2000h = ~83 days (aligns with 4H/500 window)
OHLCV_LIMIT      = 2000      # 1H OHLCV bars to fetch (covers the full Hyblock window)
HA_WINDOW_1H     = 50        # number of 1H bars for HA computation
HA_WINDOW_6H     = 20        # number of 6H bars for HA computation

# -- Fees ----------------------------------------------------------------------
FEE_PCT = 0.06   # 0.06% per side (taker), 0.12% round-trip


# ===============================================================================
# 1. Hyblock API client
# ===============================================================================

async def get_hyblock_token(client: httpx.AsyncClient) -> str:
    basic = base64.b64encode(f"{HYBLOCK_CLIENT_ID}:{HYBLOCK_SECRET}".encode()).decode()
    resp = await client.post(
        f"{HYBLOCK_BASE}/oauth2/token",
        data="grant_type=client_credentials",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type":  "application/x-www-form-urlencoded",
            "x-api-key":     HYBLOCK_API_KEY,
        },
        timeout=20.0,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


async def hyblock_get(
    client: httpx.AsyncClient,
    token: str,
    endpoint: str,
    params: Dict,
) -> List[Dict]:
    resp = await client.get(
        f"{HYBLOCK_BASE}/{endpoint}",
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key":     HYBLOCK_API_KEY,
            "Accept":        "application/json",
        },
        timeout=20.0,
    )
    if resp.status_code != 200:
        print(f"  [warn] {endpoint} returned {resp.status_code}: {resp.text[:80]}")
        return []
    body = resp.json()
    return body.get("data", []) if isinstance(body, dict) else []


async def fetch_all_hyblock(client: httpx.AsyncClient, token: str) -> Dict[str, List[Dict]]:
    p = {"coin": "BTC", "exchange": EXCHANGE, "timeframe": TIMEFRAME, "limit": str(LIMIT)}

    endpoints = [
        "openInterest",
        "volumeDelta",
        "whaleRetailDelta",
        "fundingRate",
        "topTraderPositions",
        "topTraderAccounts",
        "globalAccounts",
        "bidAsk",
    ]
    print(f"  Fetching {len(endpoints)} Hyblock endpoints ?")
    results = await asyncio.gather(
        *[hyblock_get(client, token, ep, p) for ep in endpoints],
        return_exceptions=True,
    )
    out = {}
    for ep, r in zip(endpoints, results):
        if isinstance(r, Exception):
            print(f"  [warn] {ep} exception: {r}")
            out[ep] = []
        else:
            out[ep] = r
            print(f"    {ep}: {len(r)} bars")
    return out


# ===============================================================================
# 2. OHLCV from CryptoCompare
# ===============================================================================

async def fetch_klines_1h(client: httpx.AsyncClient, limit: int = LIMIT) -> List[Dict]:
    resp = await client.get(
        f"{CC_BASE}/data/v2/histohour",
        params={"fsym": "BTC", "tsym": "USD", "limit": limit, "aggregate": 1},
        timeout=30.0,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("Response") == "Error":
        raise ValueError(body.get("Message"))
    raw = body["Data"]["Data"]
    return [
        {
            "ts": int(c["time"]),
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low":  float(c["low"]),
            "close": float(c["close"]),
            "volume": float(c["volumefrom"]),
        }
        for c in raw
        if not (c["open"] == 0 and c["close"] == 0)
    ]


# ===============================================================================
# 3. Data alignment
# ===============================================================================

def build_lookup(rows: List[Dict], key_field: str = "openDate") -> Dict[int, Dict]:
    return {int(r[key_field]): r for r in rows if key_field in r}


def merge_bars(klines: List[Dict], hyblock: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Join each 1H kline bar with Hyblock signals by nearest timestamp.
    Hyblock uses Unix seconds in 'openDate'; klines use 'ts'.
    Forward-fill missing Hyblock values.
    """
    lookups = {ep: build_lookup(rows) for ep, rows in hyblock.items()}

    # Build sorted list of Hyblock timestamps for nearest-match
    all_hb_ts: Dict[str, List[int]] = {
        ep: sorted(lk.keys()) for ep, lk in lookups.items()
    }

    def nearest(ep: str, ts: int) -> Optional[Dict]:
        ts_list = all_hb_ts.get(ep, [])
        if not ts_list:
            return None
        import bisect
        # Only consider past/current Hyblock bars (no look-ahead)
        idx = bisect.bisect_right(ts_list, ts) - 1
        if idx < 0:
            return None
        best = ts_list[idx]
        # At 4H bars, max gap from bar open to last 1H of that period = 3H = 10800s
        # Allow up to 5H (18000s) to be safe across timeframe changes
        if ts - best > 18000:
            return None
        return lookups[ep][best]

    merged = []
    for k in klines:
        ts = k["ts"]
        bar = dict(k)

        # OI
        oi = nearest("openInterest", ts) or {}
        bar["oi_close"] = oi.get("close", 0.0)
        bar["oi_open"]  = oi.get("open",  0.0)

        # Volume delta
        vd = nearest("volumeDelta", ts) or {}
        bar["volume_delta"] = vd.get("volumeDelta", 0.0)

        # Whale/retail delta
        wr = nearest("whaleRetailDelta", ts) or {}
        bar["whale_delta"] = wr.get("whaleRetailDelta", 0.0)

        # Funding rate
        fr = nearest("fundingRate", ts) or {}
        bar["funding_rate"] = fr.get("fundingRate", 0.0)

        # Top trader positions (longPct = % of top traders long)
        ttp = nearest("topTraderPositions", ts) or {}
        bar["tt_long_pct"] = ttp.get("longPct", 50.0)

        # Global accounts
        ga = nearest("globalAccounts", ts) or {}
        bar["global_long_pct"] = ga.get("longPct", 50.0)

        # Bid/ask -> OBI
        ba = nearest("bidAsk", ts) or {}
        bid = ba.get("bid", 0.0)
        ask = ba.get("ask", 0.0)
        total = bid + ask
        bar["obi"] = (bid - ask) / total if total > 0 else 0.0

        merged.append(bar)

    # Sort ascending
    merged.sort(key=lambda b: b["ts"])
    return merged


# ===============================================================================
# 4. Heikin Ashi
# ===============================================================================

def compute_ha(candles: List[Dict]) -> List[Dict]:
    """Compute Heikin Ashi for a list of {open,high,low,close} dicts."""
    ha = []
    prev_ha_open = prev_ha_close = None
    for c in candles:
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4
        if prev_ha_open is None:
            ha_open = (c["open"] + c["close"]) / 2
        else:
            ha_open = (prev_ha_open + prev_ha_close) / 2
        ha_high = max(c["high"], ha_open, ha_close)
        ha_low  = min(c["low"],  ha_open, ha_close)
        color = "GREEN" if ha_close > ha_open else "RED"
        ha.append({**c, "ha_open": ha_open, "ha_close": ha_close,
                   "ha_high": ha_high, "ha_low": ha_low, "color": color})
        prev_ha_open  = ha_open
        prev_ha_close = ha_close
    return ha


def make_6h_candles(bars_1h: List[Dict]) -> List[Dict]:
    """Group 1H bars into 6H OHLCV bars (UTC-aligned to 0,6,12,18h)."""
    groups: Dict[int, List[Dict]] = {}
    for b in bars_1h:
        dt = datetime.fromtimestamp(b["ts"], tz=timezone.utc)
        bucket = dt.replace(hour=(dt.hour // 6) * 6, minute=0, second=0, microsecond=0)
        ts6 = int(bucket.timestamp())
        groups.setdefault(ts6, []).append(b)

    out = []
    for ts6, group in sorted(groups.items()):
        group.sort(key=lambda b: b["ts"])
        out.append({
            "ts":     ts6,
            "open":   group[0]["open"],
            "high":   max(b["high"] for b in group),
            "low":    min(b["low"]  for b in group),
            "close":  group[-1]["close"],
            "volume": sum(b["volume"] for b in group),
        })
    return out


def get_candle_color(ha_candles: List[Dict]) -> str:
    return ha_candles[-1]["color"] if ha_candles else "NEUTRAL"


def get_ha_trend(ha_candles: List[Dict], lookback: int = 3) -> str:
    if len(ha_candles) < lookback:
        return "NEUTRAL"
    last = ha_candles[-lookback:]
    greens = sum(1 for c in last if c["color"] == "GREEN")
    reds   = lookback - greens
    if greens == lookback:
        return "STRONG_BULLISH"
    if reds == lookback:
        return "STRONG_BEARISH"
    if greens > reds:
        return "BULLISH"
    if reds > greens:
        return "BEARISH"
    return "NEUTRAL"


def count_consecutive_opposite(ha_candles: List[Dict], side: str) -> int:
    """Count consecutive opposing candles from the most recent bar."""
    opp = "RED" if side == "LONG" else "GREEN"
    count = 0
    for c in reversed(ha_candles):
        if c["color"] == opp:
            count += 1
        else:
            break
    return count


# ===============================================================================
# 5. Zone helpers
# ===============================================================================

def get_zone_key(price: float) -> str:
    base = int(price // ZONE_SIZE) * int(ZONE_SIZE)
    return f"${base // 1000}k"


def get_zone_position(price: float) -> str:
    lower = int(price // ZONE_SIZE) * ZONE_SIZE
    upper = lower + ZONE_SIZE
    rel   = (price - lower) / (upper - lower)
    if rel >= 0.80:
        return "TOP"
    if rel <= 0.20:
        return "BOTTOM"
    return "MID"


def zone_crossed(price: float, prev_price: float) -> Optional[str]:
    cur_z  = int(price      // ZONE_SIZE)
    prev_z = int(prev_price // ZONE_SIZE)
    if cur_z > prev_z:
        return "UP"
    if cur_z < prev_z:
        return "DOWN"
    return None


# ===============================================================================
# 6. Hyblock confidence signal
# ===============================================================================

def hyblock_score(bar: Dict, direction: str) -> Tuple[float, List[str], bool]:
    """
    Returns (score_delta, notes, should_block).
    Mirrors the logic in HyblockMonitor.get_trade_context().
    """
    delta = 0.0
    notes = []
    block = False

    # -- OBI ------------------------------------------------------------------
    obi = bar.get("obi", 0.0)
    if obi > 0.05:
        if direction == "LONG":
            delta += 8.0; notes.append("OBI?")
        else:
            delta -= 5.0
    elif obi < -0.05:
        if direction == "SHORT":
            delta += 8.0; notes.append("OBI?")
        else:
            delta -= 5.0

    # -- Whale delta -----------------------------------------------------------
    wd = bar.get("whale_delta", 0.0)
    if wd > 0.1:
        if direction == "LONG":
            delta += 5.0; notes.append("whale_bull")
        else:
            delta -= 4.0
    elif wd < -0.1:
        if direction == "SHORT":
            delta += 5.0; notes.append("whale_bear")
        else:
            delta -= 4.0

    # -- Top traders (contrarian) ----------------------------------------------
    tt_long = bar.get("tt_long_pct", 50.0)
    if tt_long > 60.0:
        if direction == "SHORT":
            delta += 5.0; notes.append("top_crowd_long->fade")
        else:
            delta -= 3.0
    elif tt_long < 40.0:
        if direction == "LONG":
            delta += 5.0; notes.append("top_crowd_short->squeeze")
        else:
            delta -= 3.0

    # -- Volume delta ----------------------------------------------------------
    vd = bar.get("volume_delta", 0.0)
    if vd > 0:
        if direction == "LONG":
            delta += 5.0; notes.append("vol_buy")
        else:
            delta -= 3.0
    elif vd < 0:
        if direction == "SHORT":
            delta += 5.0; notes.append("vol_sell")
        else:
            delta -= 3.0

    # -- OI trend (rate of change from previous bar) ---------------------------
    oi_now  = bar.get("oi_close", 0.0)
    oi_prev = bar.get("oi_open",  0.0)
    if oi_prev > 0:
        oi_roc = (oi_now - oi_prev) / oi_prev * 100
        if oi_roc > 1.0 and direction == "LONG":
            delta += 3.0; notes.append("OI_rising")
        elif oi_roc < -1.0 and direction == "SHORT":
            delta += 3.0; notes.append("OI_falling")

    # -- Cascade risk (funding + OI RoC) --------------------------------------
    fr   = abs(bar.get("funding_rate", 0.0))
    risk = 0
    if fr > 0.001:
        risk += 2
    elif fr > 0.0005:
        risk += 1
    if risk >= 2:
        delta -= 10.0
        block = True
        notes.append("CASCADE_CRITICAL")
    elif risk >= 1:
        delta -= 3.0
        notes.append("cascade_med")

    return delta, notes, block


# ===============================================================================
# 7. Confidence score
# ===============================================================================

def compute_confidence(
    direction: str,
    ha_1h_trend: str,
    zone_position: str,
    funding_rate: float,
    bar: Dict,
) -> Tuple[float, float, List[str], bool]:
    """
    Returns (confidence, hyblock_delta, notes, hyblock_block).
    """
    score = 50.0

    # HA trend
    if ha_1h_trend in ("STRONG_BULLISH", "STRONG_BEARISH"):
        score += 20.0
    elif ha_1h_trend in ("BULLISH", "BEARISH"):
        score += 10.0

    # Zone position
    if direction == "SHORT" and zone_position == "TOP":
        score += 10.0
    elif direction == "LONG" and zone_position == "BOTTOM":
        score += 10.0

    # Funding contrarian (high positive funding -> bearish; go SHORT for bonus)
    if funding_rate > 0.0003 and direction == "SHORT":
        score += 10.0
    elif funding_rate < -0.0003 and direction == "LONG":
        score += 10.0
    elif abs(funding_rate) <= 0.0001:
        score += 5.0   # neutral market

    # Hyblock signals
    hb_delta, notes, hb_block = hyblock_score(bar, direction)
    score += hb_delta
    score = min(100.0, max(0.0, score))
    return score, hb_delta, notes, hb_block


# ===============================================================================
# 8. Main simulation
# ===============================================================================

@dataclass
class Trade:
    direction:    str
    entry_price:  float
    entry_ts:     int
    entry_bar:    int
    zone:         str
    confidence:   float
    hb_notes:     List[str]
    exit_price:   float = 0.0
    exit_ts:      int   = 0
    exit_bar:     int   = 0
    exit_reason:  str   = ""
    peak_pnl:     float = 0.0
    final_pnl:    float = 0.0
    fee_pct:      float = FEE_PCT * 2   # round-trip


def pnl_pct(direction: str, entry: float, price: float) -> float:
    if direction == "LONG":
        return (price - entry) / entry * 100.0 * LEVERAGE
    return (entry - price) / entry * 100.0 * LEVERAGE


def simulate(bars: List[Dict]) -> Tuple[List[Trade], Dict]:
    """Run the full backtest. Returns (trades, stats)."""

    # Prepare 6H candles for HA
    bars_6h  = make_6h_candles(bars)
    ha_6h_all = compute_ha(bars_6h)
    # Map 6H HA candles by their close timestamp (= end of 6H bucket)
    ha6h_by_ts: Dict[int, Dict] = {}
    for c6 in ha_6h_all:
        end_ts = c6["ts"] + 6 * 3600
        for s in range(c6["ts"], end_ts, 3600):
            ha6h_by_ts[s] = c6

    # 1H HA for all bars
    ha_1h_all = compute_ha(bars)

    # State
    current_trade: Optional[Trade] = None
    zone_cooldown: Dict[str, int]  = {}   # key -> cooldown_until_ts
    zone_count:    Dict[str, int]  = {}   # key -> entries this session
    first_breaks:  Dict[str, int]  = {}   # zone_key -> ts of first break
    prev_price:    Optional[float] = None

    trades: List[Trade]  = []
    block_stats: Dict[str, int] = defaultdict(int)

    WARMUP = max(HA_WINDOW_1H, 30)   # bars needed before first signal

    for i, bar in enumerate(bars):
        ts = bar["ts"]
        close = bar["close"]
        high  = bar["high"]
        low   = bar["low"]

        # -- Update open trade ----------------------------------------------
        if current_trade is not None:
            # Track peak using intrabar high/low
            intra_best = (
                pnl_pct(current_trade.direction, current_trade.entry_price, high)
                if current_trade.direction == "LONG"
                else pnl_pct(current_trade.direction, current_trade.entry_price, low)
            )
            if intra_best > current_trade.peak_pnl:
                current_trade.peak_pnl = intra_best

            cur_pnl = pnl_pct(current_trade.direction, current_trade.entry_price, close)

            # -- Trailing stop ----------------------------------------------
            if current_trade.peak_pnl >= TP1_PCT:
                trail = TRAIL_AFTER_TP2 if current_trade.peak_pnl >= TP2_PCT else TRAIL_AFTER_TP1
                if current_trade.peak_pnl - cur_pnl >= trail:
                    current_trade.exit_price  = close
                    current_trade.exit_ts     = ts
                    current_trade.exit_bar    = i
                    current_trade.exit_reason = f"trail_stop peak={current_trade.peak_pnl:.1f}%"
                    current_trade.final_pnl   = cur_pnl - current_trade.fee_pct * LEVERAGE
                    trades.append(current_trade)
                    current_trade = None
                    prev_price    = close
                    continue

            # -- 4-candle emergency close -----------------------------------
            if i >= WARMUP:
                ha_window = ha_1h_all[max(0, i - 10): i + 1]
                opp_count = count_consecutive_opposite(ha_window, current_trade.direction)
                if opp_count >= EMERGENCY_BARS:
                    current_trade.exit_price  = close
                    current_trade.exit_ts     = ts
                    current_trade.exit_bar    = i
                    current_trade.exit_reason = f"emergency_{opp_count}bars"
                    current_trade.final_pnl   = cur_pnl - current_trade.fee_pct * LEVERAGE
                    trades.append(current_trade)
                    current_trade = None
                    prev_price    = close
                    continue

            prev_price = close
            continue   # no new entry while in trade

        # -- No open trade ? look for entry --------------------------------
        if i < WARMUP:
            prev_price = close
            continue

        # Get HA windows
        ha_1h_window = ha_1h_all[max(0, i - HA_WINDOW_1H): i + 1]
        ha_6h_cur    = ha6h_by_ts.get(ts)
        if not ha_1h_window or ha_6h_cur is None:
            prev_price = close
            continue

        ha_1h_color = get_candle_color(ha_1h_window)
        ha_6h_color = ha_6h_cur["color"]
        ha_1h_trend = get_ha_trend(ha_1h_window, lookback=3)

        # -- Step 1: HA alignment ------------------------------------------
        if ha_1h_color == "GREEN" and ha_6h_color == "GREEN":
            direction = "LONG"
        elif ha_1h_color == "RED" and ha_6h_color == "RED":
            direction = "SHORT"
        else:
            block_stats["ha_mismatch"] += 1
            prev_price = close
            continue

        # -- Step 2: Zone cooldown / cap -----------------------------------
        zone = get_zone_key(close)
        zone_pos = get_zone_position(close)
        cd_key = f"{zone}_{direction}"

        if zone_cooldown.get(cd_key, 0) > ts:
            block_stats["zone_cooldown"] += 1
            prev_price = close
            continue

        if zone_count.get(cd_key, 0) >= MAX_ZONE_ENTRIES:
            block_stats["zone_max"] += 1
            prev_price = close
            continue

        # -- Step 3: Second-break (dwarf) rule -----------------------------
        if prev_price is not None:
            cross = zone_crossed(close, prev_price)
            if cross:
                if zone not in first_breaks:
                    first_breaks[zone] = ts
                    block_stats["first_break"] += 1
                    prev_price = close
                    continue
                elif ts - first_breaks[zone] > 3600:
                    # First break expired ? reset
                    first_breaks[zone] = ts
                    block_stats["first_break"] += 1
                    prev_price = close
                    continue
                else:
                    del first_breaks[zone]   # confirmed second break ? proceed

        # -- Step 4: Velocity filter ---------------------------------------
        # Block LONG if price DUMPED >threshold% (falling knife)
        # Block SHORT if price PUMPED >threshold% (don't short momentum)
        if i >= VELOCITY_WIN:
            prev2 = bars[i - VELOCITY_WIN]["close"]
            vel_pct = (close - prev2) / prev2 * 100.0
            if direction == "LONG" and vel_pct < -VELOCITY_MIN:
                block_stats["velocity"] += 1
                prev_price = close
                continue
            if direction == "SHORT" and vel_pct > VELOCITY_MIN:
                block_stats["velocity"] += 1
                prev_price = close
                continue

        # -- Step 5: Funding rate contrarian -------------------------------
        fr = bar.get("funding_rate", 0.0)
        if direction == "LONG" and fr > 0.001:
            block_stats["funding"] += 1
            prev_price = close
            continue
        if direction == "SHORT" and fr < -0.001:
            block_stats["funding"] += 1
            prev_price = close
            continue

        # -- Step 6: Hyblock cascade risk block ----------------------------
        _, hb_notes, hb_block = hyblock_score(bar, direction)
        if hb_block:
            block_stats["hb_cascade"] += 1
            prev_price = close
            continue

        # -- Step 7: Confidence score gate --------------------------------
        confidence, hb_delta, _, _ = compute_confidence(
            direction, ha_1h_trend, zone_pos, fr, bar
        )
        if confidence < CONFIDENCE_MIN:
            block_stats["low_confidence"] += 1
            prev_price = close
            continue

        # -- Enter trade ---------------------------------------------------
        current_trade = Trade(
            direction   = direction,
            entry_price = close,
            entry_ts    = ts,
            entry_bar   = i,
            zone        = zone,
            confidence  = confidence,
            hb_notes    = hb_notes,
        )
        zone_cooldown[cd_key] = ts + ZONE_COOLDOWN_H * 3600
        zone_count[cd_key]    = zone_count.get(cd_key, 0) + 1
        prev_price = close

    # Close any open trade at the last bar
    if current_trade is not None:
        last = bars[-1]
        cur_pnl = pnl_pct(current_trade.direction, current_trade.entry_price, last["close"])
        current_trade.exit_price  = last["close"]
        current_trade.exit_ts     = last["ts"]
        current_trade.exit_bar    = len(bars) - 1
        current_trade.exit_reason = "end_of_data"
        current_trade.final_pnl   = cur_pnl - current_trade.fee_pct * LEVERAGE
        trades.append(current_trade)

    return trades, dict(block_stats)


# ===============================================================================
# 9. Reporting
# ===============================================================================

def print_results(trades: List[Trade], block_stats: Dict, bars: List[Dict]):
    print()
    print("=" * 65)
    print("  HYBLOCK STRATEGY BACKTEST RESULTS")
    print("=" * 65)

    if not trades:
        print("  No trades generated.")
        print(f"\n  Block reasons: {block_stats}")
        return

    completed = [t for t in trades if t.exit_reason != "end_of_data"]
    wins       = [t for t in completed if t.final_pnl > 0]
    losses     = [t for t in completed if t.final_pnl <= 0]
    longs      = [t for t in completed if t.direction == "LONG"]
    shorts     = [t for t in completed if t.direction == "SHORT"]

    total      = len(completed)
    win_rate   = len(wins) / total * 100 if total else 0
    avg_win    = sum(t.final_pnl for t in wins)   / len(wins)   if wins   else 0
    avg_loss   = sum(t.final_pnl for t in losses) / len(losses) if losses else 0
    avg_pnl    = sum(t.final_pnl for t in completed) / total if total else 0
    total_pnl  = sum(t.final_pnl for t in completed)
    profit_factor = abs(sum(t.final_pnl for t in wins) / sum(t.final_pnl for t in losses)) if losses and sum(t.final_pnl for t in losses) != 0 else float("inf")
    avg_conf   = sum(t.confidence for t in completed) / total if total else 0
    avg_hold   = sum(t.exit_bar - t.entry_bar for t in completed) / total if total else 0

    # Peak P&L distribution
    reached_tp1 = sum(1 for t in completed if t.peak_pnl >= TP1_PCT)
    reached_tp2 = sum(1 for t in completed if t.peak_pnl >= TP2_PCT)

    print(f"\n  Period  : {datetime.fromtimestamp(bars[0]['ts']).strftime('%Y-%m-%d')} -> "
          f"{datetime.fromtimestamp(bars[-1]['ts']).strftime('%Y-%m-%d')} ({len(bars)} 1H bars)")
    print(f"  Trades  : {total} completed  ({len(longs)} LONG / {len(shorts)} SHORT)")
    print(f"  Win Rate: {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Avg P&L : {avg_pnl:+.1f}%  (wins: {avg_win:+.1f}% / losses: {avg_loss:+.1f}%)")
    print(f"  Total P&L (cumulative): {total_pnl:+.1f}%")
    print(f"  Profit Factor: {profit_factor:.2f}x")
    print(f"  Avg Confidence: {avg_conf:.1f}/100")
    print(f"  Avg Hold Time : {avg_hold:.1f} bars ({avg_hold:.1f}h)")
    print(f"  Reached TP1 (>={TP1_PCT}%): {reached_tp1}/{total} ({reached_tp1/total*100:.0f}%)")
    print(f"  Reached TP2 (>={TP2_PCT}%): {reached_tp2}/{total} ({reached_tp2/total*100:.0f}%)")

    # Exit reason breakdown
    exit_reasons: Dict[str, int] = defaultdict(int)
    for t in completed:
        r = t.exit_reason.split("_peak")[0].split(" peak")[0]
        exit_reasons[r] += 1
    print(f"\n  Exit reasons:")
    for reason, cnt in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason:<25} {cnt:>4} ({cnt/total*100:.0f}%)")

    # Block stats
    total_blocks = sum(block_stats.values())
    print(f"\n  Entry blocks ({total_blocks} total):")
    for k, v in sorted(block_stats.items(), key=lambda x: -x[1]):
        print(f"    {k:<25} {v:>5}")

    # Hyblock signal breakdown: wins vs losses
    hb_win_notes: Dict[str, int]  = defaultdict(int)
    hb_loss_notes: Dict[str, int] = defaultdict(int)
    for t in wins:
        for n in t.hb_notes:
            hb_win_notes[n] += 1
    for t in losses:
        for n in t.hb_notes:
            hb_loss_notes[n] += 1

    all_notes = set(list(hb_win_notes.keys()) + list(hb_loss_notes.keys()))
    if all_notes:
        print(f"\n  Hyblock signals fired (wins vs losses):")
        print(f"    {'Signal':<25} {'Wins':>6} {'Losses':>7}  {'Win%':>5}")
        for note in sorted(all_notes):
            w = hb_win_notes.get(note, 0)
            l = hb_loss_notes.get(note, 0)
            total_n = w + l
            pct = w / total_n * 100 if total_n else 0
            print(f"    {note:<25} {w:>6} {l:>7}  {pct:>5.0f}%")

    # Confidence distribution
    conf_buckets = [0, 0, 0, 0, 0]  # <55, 55-65, 65-75, 75-85, 85+
    for t in completed:
        c = t.confidence
        if c < 55:   conf_buckets[0] += 1
        elif c < 65: conf_buckets[1] += 1
        elif c < 75: conf_buckets[2] += 1
        elif c < 85: conf_buckets[3] += 1
        else:        conf_buckets[4] += 1
    labels = ["<55", "55-65", "65-75", "75-85", "85+"]
    print(f"\n  Confidence score distribution:")
    for label, cnt in zip(labels, conf_buckets):
        bar_w = int(cnt / max(conf_buckets) * 30) if max(conf_buckets) else 0
        print(f"    {label:<8} {'?' * bar_w} {cnt}")

    # Per-zone breakdown
    zone_wins:  Dict[str, int] = defaultdict(int)
    zone_total: Dict[str, int] = defaultdict(int)
    for t in completed:
        zone_total[t.zone] += 1
        if t.final_pnl > 0:
            zone_wins[t.zone] += 1
    print(f"\n  Per-zone performance (top 10):")
    zone_stats = sorted(zone_total.keys(), key=lambda z: -zone_total[z])[:10]
    for z in zone_stats:
        tot = zone_total[z]
        w   = zone_wins.get(z, 0)
        print(f"    {z:<8}  {w}W/{tot-w}L  ({w/tot*100:.0f}% win)")

    print("\n" + "=" * 65)


# ===============================================================================
# 10. Entry point
# ===============================================================================

async def main():
    print("Hyblock Strategy Backtest")
    print("-" * 40)

    async with httpx.AsyncClient() as client:
        print("Authenticating with Hyblock ?")
        token = await get_hyblock_token(client)
        print("  Token OK")

        print(f"\nFetching {LIMIT} bars of Hyblock history ?")
        hyblock = await fetch_all_hyblock(client, token)

        print(f"\nFetching {OHLCV_LIMIT} bars of 1H OHLCV from CryptoCompare ?")
        klines = await fetch_klines_1h(client, limit=OHLCV_LIMIT)
        print(f"  Got {len(klines)} bars")

    print("\nAligning data ?")
    bars = merge_bars(klines, hyblock)
    print(f"  {len(bars)} merged bars ready")

    print("\nRunning simulation ?")
    trades, block_stats = simulate(bars)

    print_results(trades, block_stats, bars)


if __name__ == "__main__":
    asyncio.run(main())
