#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hyperliquid BTC Whale Address Indexer (GitHub Actions friendly)
- Connects to WS for a limited duration (DURATION_SECONDS), then exits
- Filters BTC trades >= NOTIONAL_THRESHOLD (default $10M)
- For trade users (both maker/taker), confirms open perp position via /info clearinghouseState
- Saves qualifying addresses into Redis set + per-address metadata hash
- Skips background cleaner (ephemeral run), relies on each run to re-validate

ENV:
  REDIS_URL            (required) e.g. rediss://default:password@host:port/0
  NOTIONAL_THRESHOLD   (optional) default 10000000
  DURATION_SECONDS     (optional) default 600 (10 minutes)
  REQUEST_TIMEOUT      (optional) default 5
  THROTTLE_SECONDS     (optional) default 30 (min seconds to wait before re-checking same address)
  COIN                 (optional) default "BTC"

Dependencies:
  pip install -r requirements.txt
"""

import os
import json
import time
import threading
from typing import Dict, Any, Optional

import requests
import redis
import websocket

# ───────────────────────── Config ─────────────────────────
WS_URL = "wss://api.hyperliquid.xyz/ws"
INFO_URL = "https://api.hyperliquid.xyz/info"

REDIS_URL = os.environ.get("REDIS_URL")  # must be provided via GH secrets
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required (use GitHub Actions secret). Example: rediss://default:password@host:port/0")

COIN = os.environ.get("COIN", "BTC")
NOTIONAL_THRESHOLD = float(os.environ.get("NOTIONAL_THRESHOLD", "10000000"))
DURATION_SECONDS = int(os.environ.get("DURATION_SECONDS", "600"))  # 10 minutes default for Actions job slot
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "5"))
THROTTLE_SECONDS = float(os.environ.get("THROTTLE_SECONDS", "30"))  # REST re-check throttle per address

REDIS_KEY_ACTIVE_SET = "whales:active"
REDIS_KEY_META_PREFIX = "whales:addr:"

# ───────────────────────── State ─────────────────────────
rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_last_checked: Dict[str, float] = {}   # throttle per address (avoid REST spam)
_end_time = time.time() + DURATION_SECONDS
_lock = threading.Lock()

def time_remaining() -> float:
    return max(0.0, _end_time - time.time())

# ───────────────────────── Helpers ─────────────────────────
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
    payload = {"type": "clearinghouseState", "user": user_addr}
    try:
        resp = requests.post(INFO_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"[WARN] clearinghouseState {user_addr} HTTP {resp.status_code}: {resp.text[:160]}")
    except Exception as e:
        print(f"[ERR] clearinghouseState {user_addr}: {e}")
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
    ts_ms = now_ms()
    meta_key = REDIS_KEY_META_PREFIX + addr
    with rds.pipeline() as p:
        p.sadd(REDIS_KEY_ACTIVE_SET, addr)
        p.hset(meta_key, mapping={
            "addr": addr,
            "coin": coin,
            "last_seen_ms": str(ts_ms),
            "last_notional": f"{notional:.8f}",
            "last_price": f"{price:.8f}",
        })
        # Optional small TTL on meta to auto-trim old metadata if set membership is removed elsewhere
        p.expire(meta_key, 3600)  # 1 hour
        p.execute()

# ───────────────────────── WS Handlers ─────────────────────────
def handle_trade(trade: Dict[str, Any]) -> None:
    # Respect Action timebox
    if time_remaining() <= 0:
        return

    coin = trade.get("coin", COIN)
    px = safe_float(trade.get("px"))
    sz = safe_float(trade.get("sz"))
    notional = px * sz
    users = trade.get("users") or []

    if len(users) != 2 or notional < NOTIONAL_THRESHOLD:
        return

    # Process both counterparties
    for addr in users:
        if not should_recheck(addr):
            continue
        ch = fetch_clearinghouse_state(addr)
        if not ch:
            continue
        if has_open_position(ch):
            if not rds.sismember(REDIS_KEY_ACTIVE_SET, addr):
                print(f"[NEW] {addr} qualifies — ${notional:,.0f} @ {px} on {coin}")
            record_whale_address(addr, notional, px, coin)

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as e:
        print(f"[ERR] WS json parse: {e}")
        return

    channel = data.get("channel")
    if channel == "trades":
        for tr in data.get("data") or []:
            try:
                handle_trade(tr)
            except Exception as e:
                print(f"[ERR] handle_trade: {e}")

def on_error(ws, error):
    print(f"[ERR] WS: {error}")

def on_close(ws, code, msg):
    print(f"[WS] Closed code={code} msg={msg}")

def on_open(ws):
    sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": COIN}}
    ws.send(json.dumps(sub))
    print(f"[WS] Subscribed to {COIN} trades (threshold ${NOTIONAL_THRESHOLD:,.0f}, duration {DURATION_SECONDS}s)")

def run_ws_until_deadline():
    # Loop so we can reconnect if dropped, but respect deadline
    while time_remaining() > 0:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # run_forever returns when socket closes; we break on deadline
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[ERR] WS loop: {e}")
        if time_remaining() > 0:
            time.sleep(2)

# ───────────────────────── Main ─────────────────────────
if __name__ == "__main__":
    print(f"[BOOT] COIN={COIN}  THRESHOLD=${NOTIONAL_THRESHOLD:,.0f}  DURATION={DURATION_SECONDS}s")
    print(f"[BOOT] Redis: {REDIS_URL}")

    run_ws_until_deadline()

    # Summary (final printout)
    try:
        members = sorted(list(rds.smembers(REDIS_KEY_ACTIVE_SET)))
        print(f"\n[SUMMARY] Active whale addresses in Redis: {len(members)}")
        for i, addr in enumerate(members[:30]):
            meta = rds.hgetall(REDIS_KEY_META_PREFIX + addr)
            last_ntl = meta.get("last_notional", "?")
            last_px = meta.get("last_price", "?")
            coin = meta.get("coin", COIN)
            try:
                ln = float(last_ntl)
                lp = float(last_px)
                print(f"  - {addr}  (last ${ln:,.0f} on {coin} @ {lp:,.0f})")
            except Exception:
                print(f"  - {addr}  (meta: {meta})")
        print("")
    except Exception as e:
        print(f"[ERR] summary: {e}")
