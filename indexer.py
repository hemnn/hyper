#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hyperliquid Whale Address Indexer — with Auto-Cleanup of Closed Positions

What it does
------------
- Subscribes to HL WS "trades" for one coin (e.g., BTC)
- For each trade >= NOTIONAL_THRESHOLD, extracts counterparties (addresses)
- Confirms they have an open perp position via /info clearinghouseState
- Writes addresses to Redis set + per-address meta hash:
    - coin, last_seen_ms, last_notional, last_price, liq_px, szi
- Background thread periodically re-checks all saved addresses:
    - If position for COIN is closed (szi == 0), removes address + meta
    - Otherwise refreshes last_seen_ms, liq_px, szi
- Designed to run ephemerally (DURATION_SECONDS), then prints a summary and exits

ENV (all optional except REDIS_URL)
-----------------------------------
REDIS_URL              -> redis://default:pass@host:port/0
COIN                   -> default "BTC"
NOTIONAL_THRESHOLD     -> default 10000000   (use 1000000 for 1M)
DURATION_SECONDS       -> default 1800
REQUEST_TIMEOUT        -> default 7
THROTTLE_SECONDS       -> default 15        (min secs before re-checking same addr on ADD flow)
META_TTL_SECONDS       -> default 14400     (4h meta TTL)
WHALE_SET_KEY          -> default "whales:active"
WHALE_META_PREFIX      -> default "whales:addr:"
VERBOSE                -> "1" for extra logs

# Cleanup controls
CLEANUP_INTERVAL_SEC   -> default 60        (how often to sweep the set)
CLEANUP_MAX_PARALLEL   -> default 32        (concurrent /info calls during cleanup)
CLEANUP_TIMEOUT        -> default 7         (per /info timeout during cleanup)
CLEANUP_SAMPLE_LIMIT   -> optional int; if set >0, cleanup samples up to this many addrs per sweep
CLEANUP_JITTER_SEC     -> default 5         (random jitter to avoid thundering herd if multiple runners)

Dependencies:
  pip install requests redis websocket-client
"""

import os
import json
import time
import random
import threading
from typing import Dict, Any, Optional, List, Tuple

import requests
import redis
import websocket
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────────────────────── Config ─────────────────────────
WS_URL = "wss://api.hyperliquid.xyz/ws"
INFO_URL = "https://api.hyperliquid.xyz/info"

REDIS_URL = os.environ.get("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required. Example: redis://default:password@host:port/0")

COIN = os.environ.get("COIN", "BTC").upper().strip()
NOTIONAL_THRESHOLD = float(os.environ.get("NOTIONAL_THRESHOLD", "10000000"))
DURATION_SECONDS = int(os.environ.get("DURATION_SECONDS", "1800"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "7"))
THROTTLE_SECONDS = float(os.environ.get("THROTTLE_SECONDS", "15"))
META_TTL_SECONDS = int(os.environ.get("META_TTL_SECONDS", "14400"))

WHALE_SET_KEY = os.environ.get("WHALE_SET_KEY", "whales:active")
WHALE_META_PREFIX = os.environ.get("WHALE_META_PREFIX", "whales:addr:")

VERBOSE = os.environ.get("VERBOSE", "0") == "1"

# Cleanup knobs
CLEANUP_INTERVAL_SEC = int(os.environ.get("CLEANUP_INTERVAL_SEC", "60"))
CLEANUP_MAX_PARALLEL = int(os.environ.get("CLEANUP_MAX_PARALLEL", "32"))
CLEANUP_TIMEOUT = float(os.environ.get("CLEANUP_TIMEOUT", "7"))
CLEANUP_SAMPLE_LIMIT = int(os.environ.get("CLEANUP_SAMPLE_LIMIT", "0"))  # 0 = all
CLEANUP_JITTER_SEC = int(os.environ.get("CLEANUP_JITTER_SEC", "5"))

# ───────────────────────── State ─────────────────────────
rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Session for the main (non-cleanup) requests
_session = requests.Session()
_session.headers.update({
    "User-Agent": "HL-Whale-Indexer/1.2",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
})

_last_checked: Dict[str, float] = {}     # throttle per address (add-flow)
_end_time = time.time() + DURATION_SECONDS
_stop_flag = False                       # graceful stop for threads

# Stats
_seen_trades = 0
_over_threshold = 0
_user_pairs = 0
_rest_checks = 0
_open_positions = 0
_added_addrs = 0
_clean_removed = 0
_clean_refreshed = 0
_clean_errors = 0

# ───────────────────────── Helpers ─────────────────────────
def log(msg: str) -> None:
    print(msg, flush=True)

def vlog(msg: str) -> None:
    if VERBOSE:
        print(msg, flush=True)

def time_remaining() -> float:
    return max(0.0, _end_time - time.time())

def now_ms() -> int:
    return int(time.time() * 1000)

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def should_recheck(addr: str) -> bool:
    t = time.time()
    last = _last_checked.get(addr, 0.0)
    if t - last >= THROTTLE_SECONDS:
        _last_checked[addr] = t
        return True
    return False

# ───────────────────────── HL Helpers ─────────────────────────
def fetch_clearinghouse_state(user_addr: str, timeout: float | None = None) -> Optional[Dict[str, Any]]:
    """Thread-safe /info call; caller can specify timeout."""
    global _rest_checks
    _rest_checks += 1
    payload = {"type": "clearinghouseState", "user": user_addr}
    try:
        s = requests.Session()
        s.headers.update(_session.headers)
        resp = s.post(INFO_URL, json=payload, timeout=(timeout or REQUEST_TIMEOUT))
        if resp.status_code == 200:
            return resp.json()
        else:
            vlog(f"[WARN] clearinghouseState {user_addr} HTTP {resp.status_code}: {resp.text[:160]}")
    except Exception as e:
        vlog(f"[ERR] clearinghouseState {user_addr}: {e}")
    return None

def extract_coin_pos(ch_state: Dict[str, Any], coin: str) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Returns (has_open_pos, liq_px, szi) for the target coin.
    """
    positions = ch_state.get("assetPositions") or []
    for ap in positions:
        pos = ap.get("position") or {}
        if (pos.get("coin") or "").upper() != coin.upper():
            continue
        szi = safe_float(pos.get("szi"))
        if abs(szi) <= 0.0:
            # Position exists but size 0 => treat as closed
            return False, None, None
        liq_raw = pos.get("liquidationPx")
        try:
            liq = float(liq_raw) if liq_raw is not None else None
        except Exception:
            liq = None
        return True, liq, abs(szi)
    return False, None, None

# ───────────────────────── Redis Writes ─────────────────────────
def record_whale_address(addr: str, notional: float, price: float, coin: str,
                         liq_px: Optional[float], szi: Optional[float]) -> None:
    """Add/update address in set + meta, with TTL."""
    global _added_addrs
    ts_ms = now_ms()
    meta_key = WHALE_META_PREFIX + addr
    mapping = {
        "addr": addr,
        "coin": coin.upper(),
        "last_seen_ms": str(ts_ms),
        "last_notional": f"{notional:.8f}",
        "last_price": f"{price:.8f}",
    }
    if liq_px is not None:
        mapping["liq_px"] = f"{liq_px:.8f}"
    if szi is not None:
        mapping["szi"] = f"{szi:.8f}"

    with rds.pipeline() as p:
        p.sadd(WHALE_SET_KEY, addr)
        p.hset(meta_key, mapping=mapping)
        if META_TTL_SECONDS > 0:
            p.expire(meta_key, META_TTL_SECONDS)
        p.execute()
    _added_addrs += 1

def refresh_active_meta(addr: str, liq_px: Optional[float], szi: Optional[float]) -> None:
    """Update fields for still-open positions during cleanup."""
    ts_ms = now_ms()
    meta_key = WHALE_META_PREFIX + addr
    mapping = {"last_seen_ms": str(ts_ms)}
    if liq_px is not None:
        mapping["liq_px"] = f"{liq_px:.8f}"
    if szi is not None:
        mapping["szi"] = f"{szi:.8f}"
    with rds.pipeline() as p:
        p.hset(meta_key, mapping=mapping)
        if META_TTL_SECONDS > 0:
            p.expire(meta_key, META_TTL_SECONDS)
        p.execute()

def remove_inactive_addr(addr: str) -> None:
    """Remove address from active set and delete meta."""
    with rds.pipeline() as p:
        p.srem(WHALE_SET_KEY, addr)
        p.delete(WHALE_META_PREFIX + addr)
        p.execute()

# ───────────────────────── WS Handlers ─────────────────────────
def extract_users(trade: Dict[str, Any]) -> List[str]:
    """
    HL 'trades' messages typically have `users` = [makerAddr, takerAddr] (hex 0x...).
    Be tolerant if shape differs.
    """
    u = trade.get("users")
    out: List[str] = []
    if isinstance(u, list):
        for it in u:
            if isinstance(it, str) and it.startswith("0x") and len(it) == 42:
                out.append(it)
            elif isinstance(it, dict):
                for k in ("address", "user", "trader"):
                    v = it.get(k)
                    if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                        out.append(v); break
    # de-duplicate preserving order
    seen = set(); uniq = []
    for a in out:
        if a not in seen:
            seen.add(a); uniq.append(a)
    return uniq

def handle_trade(trade: Dict[str, Any]) -> None:
    global _seen_trades, _over_threshold, _user_pairs, _open_positions
    if time_remaining() <= 0 or _stop_flag:
        return

    _seen_trades += 1
    coin = str(trade.get("coin", COIN)).upper()
    if coin != COIN:
        return

    px = safe_float(trade.get("px"))
    sz = safe_float(trade.get("sz"))
    notional = px * sz
    if notional < NOTIONAL_THRESHOLD:
        return

    _over_threshold += 1
    users = extract_users(trade)
    if not users:
        return
    _user_pairs += len(users)

    for addr in users:
        if not should_recheck(addr):
            continue
        ch = fetch_clearinghouse_state(addr, timeout=REQUEST_TIMEOUT)
        if not ch:
            continue
        has_pos, liq_px, szi = extract_coin_pos(ch, coin)
        if has_pos:
            _open_positions += 1
            record_whale_address(addr, notional, px, coin, liq_px, szi)
            log(f"[NEW] {addr}  ${notional:,.0f}  @{px:,.0f}  coin={coin}  liq={liq_px}  szi={szi}")

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as e:
        vlog(f"[ERR] WS json parse: {e}")
        return
    if data.get("channel") == "trades":
        d = data.get("data")
        if isinstance(d, list):
            for tr in d:
                try: handle_trade(tr)
                except Exception as e: vlog(f"[ERR] handle_trade: {e}")
        elif isinstance(d, dict):
            try: handle_trade(d)
            except Exception as e: vlog(f"[ERR] handle_trade(single): {e}")

def on_error(ws, error):
    vlog(f"[ERR] WS: {error}")

def on_close(ws, code, msg):
    vlog(f"[WS] Closed code={code} msg={msg}")

def on_open(ws):
    sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": COIN}}
    ws.send(json.dumps(sub))
    log(f"[WS] Subscribed coin={COIN}  threshold=${NOTIONAL_THRESHOLD:,.0f}  duration={DURATION_SECONDS}s")

def run_ws_until_deadline():
    """Reconnect loop; respects deadline and stop flag."""
    while time_remaining() > 0 and not _stop_flag:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            vlog(f"[ERR] WS loop: {e}")
        if time_remaining() > 0 and not _stop_flag:
            time.sleep(2)

# ───────────────────────── Cleanup Thread ─────────────────────────
def _cleanup_worker(addr: str) -> Tuple[str, str]:
    """Returns ('refresh'|'remove'|'error', addr)."""
    try:
        ch = fetch_clearinghouse_state(addr, timeout=CLEANUP_TIMEOUT)
        if not ch:
            return ("error", addr)
        has_pos, liq_px, szi = extract_coin_pos(ch, COIN)
        if has_pos:
            refresh_active_meta(addr, liq_px, szi)
            return ("refresh", addr)
        else:
            remove_inactive_addr(addr)
            return ("remove", addr)
    except Exception:
        return ("error", addr)

def cleanup_inactive_positions():
    """Re-check saved addresses; drop closed positions; refresh active ones."""
    global _clean_removed, _clean_refreshed, _clean_errors
    try:
        members = list(rds.smembers(WHALE_SET_KEY))
        if not members:
            return

        # Optional sampling for very large sets
        if CLEANUP_SAMPLE_LIMIT and CLEANUP_SAMPLE_LIMIT > 0 and CLEANUP_SAMPLE_LIMIT < len(members):
            members = random.sample(members, CLEANUP_SAMPLE_LIMIT)

        # Concurrent checks
        with ThreadPoolExecutor(max_workers=CLEANUP_MAX_PARALLEL) as ex:
            futs = {ex.submit(_cleanup_worker, addr): addr for addr in members}
            for fut in as_completed(futs):
                status, _addr = fut.result()
                if status == "refresh":
                    _clean_refreshed += 1
                elif status == "remove":
                    _clean_removed += 1
                else:
                    _clean_errors += 1
    except Exception as e:
        vlog(f"[ERR] cleanup sweep: {e}")

def start_cleanup_thread():
    """Run periodic cleanup until deadline/stop."""
    def loop():
        # small jitter so multiple runners don't align
        time.sleep(random.uniform(0, CLEANUP_JITTER_SEC))
        while time_remaining() > 0 and not _stop_flag:
            t0 = time.time()
            cleanup_inactive_positions()
            elapsed = max(0.0, time.time() - t0)
            sleep_left = max(1.0, CLEANUP_INTERVAL_SEC - elapsed)
            # avoid tight loop if interval is tiny
            for _ in range(int(sleep_left)):
                if _stop_flag or time_remaining() <= 0:
                    break
                time.sleep(1)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t

# ───────────────────────── Main ─────────────────────────
if __name__ == "__main__":
    log(f"[BOOT] COIN={COIN}  THRESHOLD=${NOTIONAL_THRESHOLD:,.0f}  DURATION={DURATION_SECONDS}s")
    log(f"[BOOT] Redis: {REDIS_URL}")
    try:
        rds.ping()
        vlog("[BOOT] Redis ping OK")
    except Exception as e:
        log(f"[FATAL] Redis ping failed: {e}")
        raise

    # Start cleanup thread
    clean_thread = start_cleanup_thread()

    # Run WS loop
    run_ws_until_deadline()

    # Signal stop and join cleanup thread quickly
    _stop_flag = True
    if clean_thread and clean_thread.is_alive():
        clean_thread.join(timeout=5)

    # Summary (final printout)
    try:
        members = sorted(list(rds.smembers(WHALE_SET_KEY)))
        log("\n[SUMMARY]")
        log(f"  trades_seen            = {_seen_trades}")
        log(f"  over_threshold         = {_over_threshold}")
        log(f"  user_addresses_seen    = {_user_pairs}")
        log(f"  rest_checks_total      = {_rest_checks}")
        log(f"  open_positions_added   = {_open_positions}")
        log(f"  addresses_current_set  = {len(members)} (key={WHALE_SET_KEY})")
        log(f"  cleanup_refreshed      = {_clean_refreshed}")
        log(f"  cleanup_removed        = {_clean_removed}")
        log(f"  cleanup_errors         = {_clean_errors}")
        # Show up to 30 with meta
        for addr in members[:30]:
            meta = rds.hgetall(WHALE_META_PREFIX + addr)
            ln = meta.get("last_notional", "?")
            lp = meta.get("last_price", "?")
            lq = meta.get("liq_px", "?")
            sz = meta.get("szi", "?")
            coin = meta.get("coin", COIN)
            try:
                ln_f = float(ln); lp_f = float(lp)
                log(f"    - {addr}  (last ${ln_f:,.0f} on {coin} @ {lp_f:,.0f}, liq={lq}, szi={sz})")
            except Exception:
                log(f"    - {addr}  (meta: {meta})")
        log("")
    except Exception as e:
        log(f"[ERR] summary: {e}")

