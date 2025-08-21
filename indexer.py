#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hyperliquid Whale Address Indexer (GitHub Actions friendly, robust logging)

What it does
------------
- Subscribes to HL WS "trades" for one coin (e.g., BTC)
- For each trade >= NOTIONAL_THRESHOLD, extracts both counterparties' addresses
- Confirms they currently have an open perp position via /info clearinghouseState
- Writes addresses to Redis set + per-address metadata hash (coin, last_seen_ms, last_notional, last_price)
- Designed to run ephemerally (DURATION_SECONDS), then exit with a clear summary

ENV (all optional except REDIS_URL)
-----------------------------------
REDIS_URL              -> e.g. redis://default:pass@host:port/0  (MUST MATCH your Dash app)
COIN                   -> default "BTC"      (what you subscribe to; app expects BTC from BTCUSDT)
NOTIONAL_THRESHOLD     -> default 10000000   (use 1000000 for 1M to capture more)
DURATION_SECONDS       -> default 1800       (30 min default; adjust to your schedule)
REQUEST_TIMEOUT        -> default 7
THROTTLE_SECONDS       -> default 15         (min seconds before re-checking the same address via REST)
META_TTL_SECONDS       -> default 14400      (4 hours meta TTL so Dash can see coin/last_seen_ms reliably)
WHALE_SET_KEY          -> default "whales:active"
WHALE_META_PREFIX      -> default "whales:addr:"
VERBOSE                -> "1" for extra logs

Dependencies:
  pip install requests redis websocket-client
"""

import os
import json
import time
import threading
from typing import Dict, Any, Optional, Iterable, List, Tuple

import requests
import redis
import websocket

# ───────────────────────── Config ─────────────────────────
WS_URL = "wss://api.hyperliquid.xyz/ws"
INFO_URL = "https://api.hyperliquid.xyz/info"

REDIS_URL = os.environ.get("REDIS_URL")  # MUST be provided in GitHub Secrets to match your Dash app
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required. Example: redis://default:password@host:port/0")

COIN = os.environ.get("COIN", "BTC").upper().strip()
NOTIONAL_THRESHOLD = float(os.environ.get("NOTIONAL_THRESHOLD", "10000000"))
DURATION_SECONDS = int(os.environ.get("DURATION_SECONDS", "1800"))  # default 30 mins for better hit-rate
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "7"))
THROTTLE_SECONDS = float(os.environ.get("THROTTLE_SECONDS", "15"))
META_TTL_SECONDS = int(os.environ.get("META_TTL_SECONDS", "14400"))  # 4h meta TTL

WHALE_SET_KEY = os.environ.get("WHALE_SET_KEY", "whales:active")
WHALE_META_PREFIX = os.environ.get("WHALE_META_PREFIX", "whales:addr:")

VERBOSE = os.environ.get("VERBOSE", "0") == "1"

# ───────────────────────── State ─────────────────────────
rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_session = requests.Session()
_session.headers.update({
    "User-Agent": "HL-Whale-Indexer/1.0",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
})

_last_checked: Dict[str, float] = {}     # throttle per address (avoid REST spam)
_end_time = time.time() + DURATION_SECONDS

# Stats
_seen_trades = 0
_over_threshold = 0
_user_pairs = 0
_rest_checks = 0
_open_positions = 0
_added_addrs = 0

# ───────────────────────── Helpers ─────────────────────────
def log(msg: str) -> None:
    print(msg, flush=True)

def vlog(msg: str) -> None:
    if VERBOSE:
        print(msg, flush=True)

def time_remaining() -> float:
    return max(0.0, _end_time - time.time())

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def now_ms() -> int:
    return int(time.time() * 1000)

def should_recheck(addr: str) -> bool:
    t = time.time()
    last = _last_checked.get(addr, 0.0)
    if t - last >= THROTTLE_SECONDS:
        _last_checked[addr] = t
        return True
    return False

def fetch_clearinghouse_state(user_addr: str) -> Optional[Dict[str, Any]]:
    global _rest_checks
    _rest_checks += 1
    payload = {"type": "clearinghouseState", "user": user_addr}
    try:
        resp = _session.post(INFO_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        else:
            vlog(f"[WARN] clearinghouseState {user_addr} HTTP {resp.status_code}: {resp.text[:160]}")
    except Exception as e:
        vlog(f"[ERR] clearinghouseState {user_addr}: {e}")
    return None

def has_open_position(ch_state: Dict[str, Any]) -> bool:
    positions = ch_state.get("assetPositions") or []
    for ap in positions:
        pos = ap.get("position") or {}
        szi = safe_float(pos.get("szi"))
        if abs(szi) > 0.0:
            return True
    return False

def record_whale_address(addr: str, notional: float, price: float, coin: str) -> None:
    global _added_addrs
    ts_ms = now_ms()
    meta_key = WHALE_META_PREFIX + addr
    with rds.pipeline() as p:
        p.sadd(WHALE_SET_KEY, addr)
        p.hset(meta_key, mapping={
            "addr": addr,
            "coin": coin.upper(),
            "last_seen_ms": str(ts_ms),
            "last_notional": f"{notional:.8f}",
            "last_price": f"{price:.8f}",
        })
        # Longer TTL than 1h so your Dash app can reliably read coin/last_seen_ms
        if META_TTL_SECONDS > 0:
            p.expire(meta_key, META_TTL_SECONDS)
        p.execute()
    _added_addrs += 1

def extract_users(trade: Dict[str, Any]) -> List[str]:
    """
    HL 'trades' messages usually have `users` = [makerAddr, takerAddr] (hex 0x...).
    Be tolerant if shape differs.
    """
    u = trade.get("users")
    out: List[str] = []
    if isinstance(u, list):
        for it in u:
            if isinstance(it, str) and it.startswith("0x") and len(it) == 42:
                out.append(it)
            elif isinstance(it, dict):
                # try common keys
                for k in ("address", "user", "trader"):
                    v = it.get(k)
                    if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                        out.append(v)
                        break
    # de-dupe while preserving order
    seen = set(); uniq = []
    for a in out:
        if a not in seen:
            seen.add(a); uniq.append(a)
    return uniq

# ───────────────────────── WS Handlers ─────────────────────────
def handle_trade(trade: Dict[str, Any]) -> None:
    global _seen_trades, _over_threshold, _user_pairs, _open_positions
    if time_remaining() <= 0:
        return

    _seen_trades += 1

    coin = str(trade.get("coin", COIN)).upper()
    if coin != COIN:
        # We subscribed to a single coin; skip others just in case
        return

    px = safe_float(trade.get("px"))
    sz = safe_float(trade.get("sz"))
    notional = px * sz

    if notional < NOTIONAL_THRESHOLD:
        return

    _over_threshold += 1

    users = extract_users(trade)
    if len(users) < 1:
        vlog(f"[SKIP] No users in trade >= threshold: {trade}")
        return

    _user_pairs += len(users)

    for addr in users:
        if not should_recheck(addr):
            continue
        ch = fetch_clearinghouse_state(addr)
        if not ch:
            continue
        if has_open_position(ch):
            _open_positions += 1
            record_whale_address(addr, notional, px, coin)
            log(f"[NEW] {addr}  ${notional:,.0f}  @{px:,.0f}  coin={coin}")

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as e:
        vlog(f"[ERR] WS json parse: {e}")
        return

    channel = data.get("channel")
    if channel == "trades":
        d = data.get("data")
        if isinstance(d, list):
            for tr in d:
                try:
                    handle_trade(tr)
                except Exception as e:
                    vlog(f"[ERR] handle_trade: {e}")
        elif isinstance(d, dict):
            # some servers may send single object
            try:
                handle_trade(d)
            except Exception as e:
                vlog(f"[ERR] handle_trade(single): {e}")

def on_error(ws, error):
    vlog(f"[ERR] WS: {error}")

def on_close(ws, code, msg):
    vlog(f"[WS] Closed code={code} msg={msg}")

def on_open(ws):
    sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": COIN}}
    ws.send(json.dumps(sub))
    log(f"[WS] Subscribed coin={COIN}  threshold=${NOTIONAL_THRESHOLD:,.0f}  duration={DURATION_SECONDS}s")

def run_ws_until_deadline():
    # Reconnect loop; respects deadline
    while time_remaining() > 0:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # Note: ping keeps the connection healthy
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            vlog(f"[ERR] WS loop: {e}")
        if time_remaining() > 0:
            time.sleep(2)

# ───────────────────────── Main ─────────────────────────
if __name__ == "__main__":
    log(f"[BOOT] COIN={COIN}  THRESHOLD=${NOTIONAL_THRESHOLD:,.0f}  DURATION={DURATION_SECONDS}s")
    log(f"[BOOT] Redis: {REDIS_URL}")
    try:
        # Quick sanity: write/read ping
        rds.ping()
        vlog("[BOOT] Redis ping OK")
    except Exception as e:
        log(f"[FATAL] Redis ping failed: {e}")
        raise

    run_ws_until_deadline()

    # Summary (final printout)
    try:
        members = sorted(list(rds.smembers(WHALE_SET_KEY)))
        log("\n[SUMMARY]")
        log(f"  trades_seen           = {_seen_trades}")
        log(f"  over_threshold        = {_over_threshold}")
        log(f"  user_addresses_seen   = {_user_pairs}")
        log(f"  rest_checks           = {_rest_checks}")
        log(f"  open_positions        = {_open_positions}")
        log(f"  addresses_added       = {_added_addrs}")
        log(f"  redis_set_memberships = {len(members)} (key={WHALE_SET_KEY})")
        for i, addr in enumerate(members[:30]):
            meta = rds.hgetall(WHALE_META_PREFIX + addr)
            last_ntl = meta.get("last_notional", "?")
            last_px = meta.get("last_price", "?")
            coin = meta.get("coin", COIN)
            try:
                ln = float(last_ntl); lp = float(last_px)
                log(f"    - {addr}  (last ${ln:,.0f} on {coin} @ {lp:,.0f})")
            except Exception:
                log(f"    - {addr}  (meta: {meta})")
        log("")
    except Exception as e:
        log(f"[ERR] summary: {e}")
