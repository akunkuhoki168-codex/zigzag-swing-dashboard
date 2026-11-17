#!/usr/bin/env python3
"""Publisher for the public ADX Swing alert dashboard (GitHub Pages).

Reads the ADX Swing bot log/state in /workspace/adx-swing-bot, recomputes
current zigzag swing state from Binance USD-M Futures public klines, and
runs a read-only copy of the 1-minute ADX scanner. Never writes
/workspace/adx/state.json (that file belongs to the alert routine).
Never starts, stops, or signals other bots.

Usage: python3 push.py [--once] [--no-push]
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

BOT = Path("/workspace/adx-swing-bot")
BOT_DIR = BOT / "bot"
LOG = BOT / "run" / "bot.log"
STATE_FILE = BOT_DIR / "state.json"
CFG_FILE = BOT_DIR / "config.yaml"
BOT_PIDF = BOT / "run" / "bot.pid"
SUP_PIDF = BOT / "run" / "supervisor.pid"
ADX_STATE = Path("/workspace/adx/state.json")  # read-only
ALERTS_JSONL = BOT / "alerts.jsonl"
ALERTS_UNCONFIRMED_JSONL = BOT / "alerts_unconfirmed.jsonl"
PAPER_STATE = BOT / "paper_state.json"
PAPER_TRADES = BOT / "paper_trades.jsonl"
PAPER_STATE_UNCONF = BOT / "paper_state_unconfirmed.json"
PAPER_TRADES_UNCONF = BOT / "paper_trades_unconfirmed.jsonl"

HERE = Path(os.path.dirname(os.path.abspath(__file__)))
STATE = HERE / "state"
ALERTS_FILE = STATE / "alerts.json"
HASH_FILE = STATE / "last_payload_hash.txt"
WIB = timezone(timedelta(hours=7))
PUSH_EVERY_S = 120
DATA_MSG = "data snapshot"
PUSHER_NAME = "adx-swing-pusher"
PUSHER_EMAIL = "pusher@users.noreply.github.com"
CHART_URL = "https://dedkovic441-hub.github.io/adx-swing/"
REST_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]
UA = "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/120.0.0.0"
ADX_COINS = ["NEARUSDT", "INJUSDT", "RENDERUSDT", "FETUSDT", "ARBUSDT"]
ADX_TF = "1m"
ADX_N = 14

sys.path.insert(0, str(BOT_DIR))
from swing import detect_swing_signals  # noqa: E402


def log(*a) -> None:
    print(datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"), *a, flush=True)


def now_wib() -> str:
    return datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S Asia/Jakarta")


def wib_from_sec(sec) -> str | None:
    if sec is None:
        return None
    try:
        return datetime.fromtimestamp(int(sec), WIB).strftime("%Y-%m-%d %H:%M:%S Asia/Jakarta")
    except (OSError, OverflowError, ValueError):
        return None


def wib_from_ms(ms) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, WIB).strftime("%Y-%m-%d %H:%M:%S Asia/Jakarta")
    except (OSError, OverflowError, ValueError):
        return None


def fmt_price(n) -> str | None:
    if not isinstance(n, (int, float)):
        return None
    a = abs(n)
    if a >= 1000:
        s = f"{n:.2f}"
    elif a >= 1:
        s = f"{n:.4f}"
    elif a >= 0.01:
        s = f"{n:.6f}"
    else:
        s = f"{n:.8f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def rest_get(path: str, timeout: float = 15):
    err = None
    for base in REST_BASES:
        url = base + "/fapi/v1" + path
        for attempt in range(8):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.load(r)
            except Exception as e:
                err = e
                msg = str(e)
                if any(code in msg for code in ("403", "202", "502", "503", "504")) and attempt < 7:
                    time.sleep(0.8 * (attempt + 1))
                    continue
                break
    raise err


def klines(symbol: str, tf: str, limit: int = 1000) -> list[dict]:
    raw = rest_get(f"/klines?symbol={symbol}&interval={tf}&limit={limit}", timeout=20)
    out = []
    for k in raw:
        out.append({
            "time": int(k[0]) // 1000,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
        })
    return out


def ticker_prices(symbols: list[str]) -> dict[str, float]:
    out = {}
    try:
        rows = rest_get("/ticker/price", timeout=12)
        want = set(symbols)
        for row in rows or []:
            s = row.get("symbol")
            if s in want:
                try:
                    out[s] = float(row["price"])
                except (TypeError, ValueError, KeyError):
                    pass
    except Exception as e:
        log("ticker failed", repr(e)[:200])
    return out


def _simple_yaml(text: str) -> dict:
    """Tiny subset parser for config.yaml (lists, scalars, comments)."""
    data = {}
    key = None
    quotes = "\'" + '"'
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith("  - ") or line.startswith("\t- "):
            if key:
                data.setdefault(key, [])
                if not isinstance(data[key], list):
                    data[key] = []
                data[key].append(line.strip()[2:].strip().strip(quotes))
            continue
        if ":" in line and not line.startswith(" "):
            k, _, v = line.partition(":")
            key = k.strip()
            v = v.strip().strip(quotes)
            data[key] = v if v else []
    return data


def load_yaml_cfg() -> dict:
    cfg = {"symbols": ["PUMPUSDT", "BTCUSDT", "ETHUSDT"], "timeframes": ["5m", "15m"], "sensitivity": "medium"}
    try:
        text = CFG_FILE.read_text()
        try:
            import yaml
            data = yaml.safe_load(text) or {}
        except Exception:
            data = _simple_yaml(text)
        if isinstance(data, dict):
            if data.get("symbols"):
                cfg["symbols"] = [str(s).replace("/", "").upper() for s in data["symbols"]]
            if data.get("timeframes"):
                cfg["timeframes"] = [str(t).lower() for t in data["timeframes"]]
            if data.get("sensitivity"):
                cfg["sensitivity"] = str(data["sensitivity"]).lower()
            if data.get("chart_url"):
                cfg["chart_url"] = str(data["chart_url"])
    except Exception as e:
        log("config read failed", repr(e)[:200])
    return cfg


def pid_alive(pid: int | None, needle: str) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="ignore")
        return needle in cmd
    except OSError:
        return False


def read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def find_proc(needle: str) -> int | None:
    try:
        for pid_s in os.listdir("/proc"):
            if not pid_s.isdigit():
                continue
            try:
                cmd = open(f"/proc/{pid_s}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="ignore")
            except OSError:
                continue
            if needle in cmd:
                return int(pid_s)
    except OSError:
        pass
    return None


def bot_runtime() -> dict:
    bot_pid = read_pid(BOT_PIDF)
    sup_pid = read_pid(SUP_PIDF)
    bot_ok = pid_alive(bot_pid, "signal_bot.py")
    if not bot_ok:
        found = find_proc("/workspace/adx-swing-bot/bot/signal_bot.py")
        if found:
            bot_pid, bot_ok = found, True
    sup_ok = pid_alive(sup_pid, "supervisor.sh")
    if not sup_ok:
        found = find_proc("/workspace/adx-swing-bot/run/supervisor.sh")
        if found:
            sup_pid, sup_ok = found, True
    return {"bot_pid": bot_pid, "supervisor_pid": sup_pid, "alive": bot_ok, "supervisor_alive": sup_ok}


def parse_log() -> dict:
    info = {
        "last_line_wib": None,
        "last_closed": None,
        "last_closed_wib": None,
        "mode": None,
        "started_wib": None,
        "alerts": [],
        "ws_error": None,
    }
    if not LOG.exists():
        return info
    try:
        text = LOG.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return info
    # keep the tail large enough for recent alerts
    lines = text.splitlines()
    if len(lines) > 20000:
        lines = lines[-20000:]
    ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    alert_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).* alert (\S+) (\S+) (\S+)")
    closed_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).* closed candle (\S+) (\S+) (.+) close=(\S+)")
    printed = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) Asia/Jakarta\]$")
    printed_body = re.compile(r"^(\S+) (\S+) (BUY\??|SELL\??) @ (\S+) \((bottom|top)\)")
    pending_print = None
    alerts = []
    for i, line in enumerate(lines):
        m = ts_re.match(line)
        if m:
            info["last_line_wib"] = m.group(1) + " Asia/Jakarta"
        if "starting signal_bot.py" in line or "coin-chart signal bot starting" in line or "ADX Swing" in line and "starting" in line:
            if m:
                info["started_wib"] = m.group(1) + " Asia/Jakarta"
        if "WebSocket live" in line:
            info["mode"] = "websocket"
            info["ws_error"] = None
        if "Live (polling)" in line or "falling back to REST polling" in line:
            info["mode"] = "polling"
        if "WebSocket error" in line or "WebSocket not live" in line:
            info["ws_error"] = line[19:180] if len(line) > 19 else line
        cm = closed_re.match(line)
        if cm:
            info["last_closed"] = f"{cm.group(2)} {cm.group(3)} close={cm.group(5)}"
            # closed-candle timestamp in the message is already Jakarta
            close_ts = cm.group(4).strip()
            info["last_closed_wib"] = close_ts if "Asia/Jakarta" in close_ts else close_ts + " Asia/Jakarta"
            info["heartbeat_wib"] = cm.group(1) + " Asia/Jakarta"
        am = alert_re.match(line)
        if am:
            alerts.append({
                "wib": am.group(1) + " Asia/Jakarta",
                "symbol": am.group(2),
                "tf": am.group(3),
                "name": am.group(4),
                "source": "log",
            })
        pm = printed.match(line)
        if pm:
            pending_print = pm.group(1)
            continue
        if pending_print:
            bm = printed_body.match(line)
            if bm:
                alerts.append({
                    "wib": pending_print + " Asia/Jakarta",
                    "symbol": bm.group(1),
                    "tf": bm.group(2),
                    "name": bm.group(3),
                    "price": bm.group(4),
                    "kind": bm.group(5),
                    "source": "print",
                })
            pending_print = None
    # dedupe printed + log.info pairs (same minute/symbol/tf/name)
    seen = set()
    out = []
    for a in alerts:
        key = (a.get("wib"), a.get("symbol"), a.get("tf"), a.get("name"))
        if key in seen:
            # prefer the one with price
            if a.get("price"):
                for j, prev in enumerate(out):
                    if (prev.get("wib"), prev.get("symbol"), prev.get("tf"), prev.get("name")) == key:
                        if not prev.get("price"):
                            out[j] = a
                        break
            continue
        seen.add(key)
        out.append(a)
    confirmed, unconfirmed = [], []
    for a in out:
        name = str(a.get("name") or "")
        if name.endswith("?") or "forming" in name.lower() or name in {"BUY?", "SELL?"}:
            unconfirmed.append(a)
        else:
            confirmed.append(a)
    info["alerts"] = confirmed[-80:]
    info["alerts_unconfirmed"] = unconfirmed[-80:]
    return info


def load_saved_alerts() -> list:
    try:
        return json.loads(ALERTS_FILE.read_text())
    except Exception:
        return []


def save_alerts(rows: list) -> list:
    STATE.mkdir(parents=True, exist_ok=True)
    # keep last 200
    rows = rows[-200:]
    tmp = str(ALERTS_FILE) + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(rows, fh)
    os.replace(tmp, ALERTS_FILE)
    return rows


def merge_alerts(saved: list, fresh: list) -> list:
    def key(a):
        return (a.get("wib"), a.get("symbol"), a.get("tf"), a.get("name"), str(a.get("price") or ""))
    seen = {key(a) for a in saved}
    out = list(saved)
    for a in fresh:
        k = key(a)
        # also merge if same event without price
        weak = (a.get("wib"), a.get("symbol"), a.get("tf"), a.get("name"))
        matched = False
        for i, prev in enumerate(out):
            if (prev.get("wib"), prev.get("symbol"), prev.get("tf"), prev.get("name")) == weak:
                if a.get("price") and not prev.get("price"):
                    out[i] = {**prev, **a}
                matched = True
                break
        if not matched and k not in seen:
            out.append(a)
            seen.add(k)
    out.sort(key=lambda a: a.get("wib") or "")
    return out


def pack_signal(sig: dict | None) -> dict | None:
    if not sig:
        return None
    return {
        "name": sig.get("name"),
        "price": sig.get("price"),
        "price_txt": fmt_price(sig.get("price")),
        "kind": sig.get("swingKind"),
        "time": int(sig["time"]) if sig.get("time") is not None else None,
        "confirm_time": None if sig.get("provisional") else (int(sig["confirmTime"]) if sig.get("confirmTime") is not None else None),
        "pivot_wib": wib_from_sec(sig.get("time")),
        "confirm_wib": wib_from_sec(sig.get("confirmTime")) if not sig.get("provisional") else None,
        "lag": sig.get("lag"),
        "stars": sig.get("stars"),
        "forming": bool(sig.get("provisional") or sig.get("forming")),
    }


def _one_market(sym: str, tf: str, prices: dict, sensitivity: str) -> dict:
    item = {
        "symbol": sym,
        "tf": tf,
        "price": prices.get(sym),
        "price_txt": fmt_price(prices.get(sym)),
        "url": f"{CHART_URL}?symbol={sym}&tf={tf}",
        "error": None,
        "last_signal": None,
        "forming": None,
        "recent_signals": [],
        "bars": None,
        "last_bar_wib": None,
    }
    try:
        candles = klines(sym, tf, 1000)
        if candles:
            item["bars"] = len(candles)
            item["last_bar_wib"] = wib_from_sec(candles[-1]["time"])
            last_closed = candles[-2] if len(candles) >= 2 else candles[-1]
            if sym not in prices:
                item["price"] = last_closed["close"]
                item["price_txt"] = fmt_price(last_closed["close"])
        pack = detect_swing_signals(candles, tf, sensitivity, True)
        sigs = pack.get("signals") or []
        item["last_signal"] = pack_signal(sigs[-1] if sigs else None)
        item["forming"] = pack_signal(pack.get("provisional"))
        item["recent_signals"] = [pack_signal(s) for s in sigs[-20:]]
    except Exception as e:
        item["error"] = repr(e)[:200]
        log("swing", sym, tf, item["error"])
    return item


def build_markets(cfg: dict, prices: dict) -> list:
    jobs = [(sym, tf) for sym in cfg["symbols"] for tf in cfg["timeframes"]]
    sensitivity = cfg.get("sensitivity") or "medium"
    found = {}
    if not jobs:
        return []
    with ThreadPoolExecutor(max_workers=min(8, len(jobs))) as ex:
        futs = {ex.submit(_one_market, sym, tf, prices, sensitivity): (sym, tf) for sym, tf in jobs}
        for fut in as_completed(futs, timeout=180):
            sym, tf = futs[fut]
            try:
                found[(sym, tf)] = fut.result()
            except Exception as e:
                found[(sym, tf)] = {
                    "symbol": sym, "tf": tf, "price": prices.get(sym),
                    "price_txt": fmt_price(prices.get(sym)),
                    "url": f"{CHART_URL}?symbol={sym}&tf={tf}",
                    "error": repr(e)[:200], "last_signal": None, "forming": None,
                    "bars": None, "last_bar_wib": None,
                }
                log("swing", sym, tf, repr(e)[:200])
    return [found[job] for job in jobs if job in found]


def load_jsonl_alerts() -> list:
    """Confirmed alerts written by the bot for the forwarder routine."""
    rows = []
    try:
        if not ALERTS_JSONL.exists():
            return rows
        for line in ALERTS_JSONL.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            detected = rec.get("detected_at_utc") or rec.get("candle_close_utc")
            wib = None
            try:
                if detected:
                    ts = detected.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ts)
                    wib = dt.astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S Asia/Jakarta")
            except Exception:
                wib = detected
            rows.append({
                "wib": wib,
                "symbol": rec.get("symbol"),
                "tf": rec.get("timeframe") or rec.get("tf"),
                "name": rec.get("side"),
                "price": rec.get("price"),
                "source": "jsonl",
                "candle_close_utc": rec.get("candle_close_utc"),
                "detected_at_utc": rec.get("detected_at_utc"),
            })
    except OSError as e:
        log("alerts.jsonl read failed", repr(e)[:200])
    return rows


def load_jsonl_alerts_unconfirmed() -> list:
    """Unconfirmed BUY?/SELL? lines. Kept separate from alerts.jsonl."""
    rows = []
    try:
        if not ALERTS_UNCONFIRMED_JSONL.exists():
            return rows
        for line in ALERTS_UNCONFIRMED_JSONL.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            detected = rec.get("detected_at_utc") or rec.get("candle_close_utc")
            wib = None
            try:
                if detected:
                    ts = detected.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ts)
                    wib = dt.astimezone(WIB).strftime("%Y-%m-%d %H:%M:%S Asia/Jakarta")
            except Exception:
                wib = detected
            rows.append({
                "wib": wib,
                "symbol": rec.get("symbol"),
                "tf": rec.get("timeframe") or rec.get("tf"),
                "name": rec.get("side"),
                "price": rec.get("price"),
                "pivot_price": rec.get("pivot_price"),
                "pivot_time": rec.get("pivot_time"),
                "source": "jsonl_unconfirmed",
                "candle_close_utc": rec.get("candle_close_utc"),
                "detected_at_utc": rec.get("detected_at_utc"),
                "unconfirmed": True,
            })
    except OSError as e:
        log("alerts_unconfirmed.jsonl read failed", repr(e)[:200])
    return rows


def wilder(x, n):
    out = [None] * len(x)
    s = sum(x[1:n + 1])
    out[n] = s
    for i in range(n + 1, len(x)):
        s = s - s / n + x[i]
        out[i] = s
    return out


def adx_from_klines(k):
    k = k[:-1]  # closed candles only
    h = [float(c["high"]) for c in k]
    l = [float(c["low"]) for c in k]
    c_ = [float(c["close"]) for c in k]
    tr = [0]
    pdm = [0]
    mdm = [0]
    for i in range(1, len(k)):
        tr.append(max(h[i] - l[i], abs(h[i] - c_[i - 1]), abs(l[i] - c_[i - 1])))
        up = h[i] - h[i - 1]
        dn = l[i - 1] - l[i]
        pdm.append(up if up > dn and up > 0 else 0)
        mdm.append(dn if dn > up and dn > 0 else 0)
    a, p, m = wilder(tr, ADX_N), wilder(pdm, ADX_N), wilder(mdm, ADX_N)
    pdi = [None] * len(k)
    mdi = [None] * len(k)
    dx = [None] * len(k)
    for i in range(ADX_N, len(k)):
        pdi[i] = 100 * p[i] / a[i]
        mdi[i] = 100 * m[i] / a[i]
        dx[i] = 100 * abs(pdi[i] - mdi[i]) / (pdi[i] + mdi[i]) if pdi[i] + mdi[i] else 0
    ad = [None] * len(k)
    ad[2 * ADX_N - 1] = sum(dx[ADX_N:2 * ADX_N]) / ADX_N
    for i in range(2 * ADX_N, len(k)):
        ad[i] = (ad[i - 1] * (ADX_N - 1) + dx[i]) / ADX_N
    close_ms = k[-1].get("close_time")
    if close_ms is None:
        close_ms = (k[-1]["time"] + 60) * 1000 - 1
    return close_ms, c_[-1], pdi, mdi, ad


def scan_adx_readonly() -> list:
    """Same rules as /workspace/adx/scan.py, but never writes that state file."""
    routine_seen = {}
    try:
        routine_seen = json.loads(ADX_STATE.read_text())
    except Exception:
        routine_seen = {}
    res = []
    for s in ADX_COINS:
        try:
            candles = klines(s, ADX_TF, 300)
            t, price, pdi, mdi, ad = adx_from_klines(candles)
        except Exception as e:
            res.append({"symbol": s, "error": repr(e)[:200]})
            continue
        sig = None
        i = len(ad) - 1
        for j in range(len(ad) - 5, len(ad)):
            if ad[j] is None or ad[j - 1] is None:
                continue
            if ad[j] >= 25 and ad[j] > ad[j - 1]:
                if pdi[j] > mdi[j] and (pdi[j - 1] <= mdi[j - 1] or ad[j - 1] < 25):
                    sig = "LONG"
                    i = j
                if mdi[j] > pdi[j] and (mdi[j - 1] <= pdi[j - 1] or ad[j - 1] < 25):
                    sig = "SHORT"
                    i = j
        t_sig = t - (len(ad) - 1 - i) * 60000
        key = f"{s}:{t_sig}"
        new = bool(sig and routine_seen.get(s) != key)
        res.append({
            "symbol": s,
            "close": price,
            "price_txt": fmt_price(price),
            "adx": round(ad[i], 1) if ad[i] is not None else None,
            "adx_prev": round(ad[i - 1], 1) if ad[i - 1] is not None else None,
            "+di": round(pdi[i], 1) if pdi[i] is not None else None,
            "-di": round(mdi[i], 1) if mdi[i] is not None else None,
            "signal": sig,
            "new_alert": new,
            "candle_close_wib": wib_from_ms(t_sig + 1),
            "url": f"{CHART_URL}?symbol={s}&tf=1m",
        })
    return res



def load_closed_paper_trades(limit: int = 40, path: Path | None = None, actions: tuple | None = None) -> list:
    rows = []
    trades_path = path or PAPER_TRADES
    want = set(actions) if actions else {"CLOSE"}
    try:
        if not trades_path.exists():
            return rows
        for line in trades_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("action") not in want:
                continue
            rec["url"] = f"{CHART_URL}?symbol={rec.get('symbol')}&tf={rec.get('timeframe')}"
            rec["entry_txt"] = fmt_price(rec.get("entry") or rec.get("avg_entry"))
            rec["exit_txt"] = fmt_price(rec.get("exit") or rec.get("price"))
            rec["price_txt"] = fmt_price(rec.get("price") or rec.get("exit"))
            rec["pnl_txt"] = None if rec.get("pnl") is None else f"{rec['pnl']:+.4f}"
            rows.append(rec)
    except OSError as e:
        log("paper_trades read failed", repr(e)[:200])
    return rows[-limit:]


CONFIRMED_PAPER_NOTE = (
    "Paper only — no real orders or API keys. $50 notional at 1x, 0.05% taker fee per side, "
    "fill at the confirmation candle close."
)
UNCONFIRMED_PAPER_NOTE = (
    "Paper only — no real orders or API keys. Default $50 notional at 1x, 0.05% taker fee per side. "
    "Enters at the close of the candle when a new unconfirmed BUY?/SELL? first appears. "
    "If that signal fades (repaints), the position stays open. On 15m, every new same-direction "
    "unconfirmed signal (including a pivot walk) adds another leg of the first leg's notional, "
    "with no fade required. On 5m, a fade is required before a same-direction add. "
    "Up to 4 adds (5 legs). An opposite unconfirmed signal closes the whole stack and flips."
)


def build_paper(prices: dict, state_path: Path | None = None, trades_path: Path | None = None, note: str | None = None, mode: str = "confirmed") -> dict:
    path = state_path or PAPER_STATE
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        st = {
            "starting_balance": 1000.0, "balance": 1000.0, "realized_pnl": 0.0,
            "fees_paid": 0.0, "notional": 50.0, "leverage": 1.0, "taker_fee": 0.0005,
            "positions": {}, "closed_count": 0, "win_count": 0,
        }
    fee_rate = float(st.get("taker_fee") or 0.0005)
    unrealized = 0.0
    positions = []
    for p in (st.get("positions") or {}).values():
        sym = p.get("symbol")
        entry = float(p.get("entry") or 0)
        qty = float(p.get("qty") or 0)
        side = p.get("side")
        mark = prices.get(sym)
        if mark is None:
            mark = entry
        direction = 1.0 if side == "LONG" else -1.0
        gross = (float(mark) - entry) * qty * direction
        upnl = gross - (qty * float(mark) * fee_rate)
        unrealized += upnl
        legs_out = []
        for leg in (p.get("legs") or []):
            legs_out.append({
                "price": leg.get("price"),
                "price_txt": fmt_price(leg.get("price")),
                "qty": leg.get("qty"),
                "notional": leg.get("notional"),
                "opened_at_wib": leg.get("opened_at_wib"),
                "opened_at_utc": leg.get("opened_at_utc"),
            })
        leg_count = int(p.get("leg_count") or len(legs_out) or 1)
        positions.append({
            "symbol": sym,
            "timeframe": p.get("timeframe"),
            "side": side,
            "qty": qty,
            "entry": entry,
            "entry_txt": fmt_price(entry),
            "avg_entry": entry,
            "avg_entry_txt": fmt_price(entry),
            "mark": float(mark),
            "mark_txt": fmt_price(mark),
            "margin": p.get("margin"),
            "notional": p.get("notional"),
            "first_notional": p.get("first_notional"),
            "leg_count": leg_count,
            "faded": bool(p.get("faded")),
            "fade_count": int(p.get("fade_count") or 0),
            "last_fade_wib": p.get("last_fade_wib"),
            "legs": legs_out,
            "unrealized": round(upnl, 4),
            "opened_at_wib": p.get("opened_at_wib"),
            "opened_at_utc": p.get("opened_at_utc"),
            "url": f"{CHART_URL}?symbol={sym}&tf={p.get('timeframe')}",
        })
    wallet = float(st.get("balance") or 0)
    realized = float(st.get("realized_pnl") or 0)
    closed = int(st.get("closed_count") or 0)
    wins = int(st.get("win_count") or 0)
    starting = float(st.get("starting_balance") or 1000)
    trades_file = trades_path or PAPER_TRADES
    closed_trades = list(reversed(load_closed_paper_trades(40, path=trades_file)))
    events = []
    if mode == "unconfirmed":
        events = list(reversed(load_closed_paper_trades(
            60, path=trades_file, actions=("CLOSE", "FADED", "ADD", "REPAINTED"),
        )))
    return {
        "starting_balance": starting,
        "balance": round(wallet, 4),
        "equity": round(wallet + unrealized, 4),
        "realized_pnl": round(realized, 4),
        "unrealized_pnl": round(unrealized, 4),
        "fees_paid": round(float(st.get("fees_paid") or 0), 4),
        "win_rate": None if closed == 0 else round(100.0 * wins / closed, 1),
        "closed_count": closed,
        "win_count": wins,
        "open_count": len(positions),
        "notional": float(st.get("notional") or 50),
        "leverage": float(st.get("leverage") or 1),
        "taker_fee": fee_rate,
        "positions": positions,
        "closed_trades": closed_trades,
        "events": events or closed_trades,
        "updated_wib": st.get("updated_wib"),
        "mode": mode,
        "max_add_positions": 4 if mode == "unconfirmed" else 0,
        "note": note or (UNCONFIRMED_PAPER_NOTE if mode == "unconfirmed" else CONFIRMED_PAPER_NOTE),
    }


def build() -> dict:
    cfg = load_yaml_cfg()
    runtime = bot_runtime()
    loginfo = parse_log()
    symbols = list(dict.fromkeys(list(cfg["symbols"]) + ADX_COINS))
    prices = ticker_prices(symbols)
    markets = build_markets(cfg, prices)
    adx_rows = scan_adx_readonly()
    alerts = merge_alerts(load_saved_alerts(), (loginfo.get("alerts") or []) + load_jsonl_alerts())
    # never let unconfirmed (BUY?/SELL?) rows mix into the confirmed list or alerts.json
    alerts = [a for a in alerts if not str(a.get("name") or "").endswith("?")]
    # attach chart urls
    for a in alerts:
        if a.get("symbol") and a.get("tf"):
            a["url"] = f"{CHART_URL}?symbol={a['symbol']}&tf={a['tf']}"
    save_alerts(alerts)

    alerts_unconfirmed = merge_alerts(
        [],
        (loginfo.get("alerts_unconfirmed") or []) + load_jsonl_alerts_unconfirmed(),
    )
    for a in alerts_unconfirmed:
        a["unconfirmed"] = True
        if a.get("symbol") and a.get("tf"):
            a["url"] = f"{CHART_URL}?symbol={a['symbol']}&tf={a['tf']}"

    alive = runtime["alive"]
    heartbeat = loginfo.get("heartbeat_wib") or loginfo.get("last_line_wib") or now_wib()
    mode = loginfo.get("mode") or ("starting" if alive else "down")
    # stale if the bot process is down, or no log line for 15 minutes
    stale = False
    if alive:
        try:
            hb = heartbeat.replace(" Asia/Jakarta", "")
            hb_ts = datetime.strptime(hb, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WIB).timestamp()
            # 5m/15m bot may be quiet between closed candles; process-alive is enough.
            if time.time() - hb_ts > 20 * 60:
                stale = True
        except Exception:
            pass
    else:
        stale = True

    state_updated = None
    try:
        st = json.loads(STATE_FILE.read_text())
        state_updated = st.get("updated")
    except Exception:
        pass

    paper = build_paper(prices, PAPER_STATE, PAPER_TRADES, CONFIRMED_PAPER_NOTE, "confirmed")
    paper_unconfirmed = build_paper(
        prices, PAPER_STATE_UNCONF, PAPER_TRADES_UNCONF, UNCONFIRMED_PAPER_NOTE, "unconfirmed",
    )

    return {
        "generated_at": now_wib(),
        "generated_ts": int(time.time()),
        "bot": {
            "name": "ADX Swing",
            "market": "Binance USD-M Futures",
            "alive": alive,
            "stale": stale and not alive,
            "pid": runtime["bot_pid"],
            "supervisor_pid": runtime["supervisor_pid"],
            "supervisor_alive": runtime["supervisor_alive"],
            "mode": mode,
            "heartbeat_wib": heartbeat,
            "last_closed": loginfo.get("last_closed"),
            "last_closed_wib": loginfo.get("last_closed_wib"),
            "started_wib": loginfo.get("started_wib"),
            "state_updated": state_updated,
            "sensitivity": cfg.get("sensitivity"),
            "symbols": cfg.get("symbols"),
            "timeframes": cfg.get("timeframes"),
            "telegram": False,
            "chart_url": CHART_URL,
            "ws_error": loginfo.get("ws_error"),
        },
        "markets": markets,
        "paper": paper,
        "paper_unconfirmed": paper_unconfirmed,
        "alerts": list(reversed(alerts[-60:])),
        "alerts_unconfirmed": list(reversed(alerts_unconfirmed[-60:])),
        "adx": {
            "tf": ADX_TF,
            "period": ADX_N,
            "threshold": 25,
            "coins": ADX_COINS,
            "note": "Same rules as the 1-minute ADX scanner (ADX ≥ 25 and rising, fresh +DI/−DI cross). Read-only; does not change the alert routine's state.",
            "rows": adx_rows,
        },
    }


def payload_hash(d: dict) -> str:
    skip = {"generated_at", "generated_ts"}
    body = {k: v for k, v in d.items() if k not in skip}
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


def git(*a, check=True) -> str:
    r = subprocess.run(["git", "-C", str(HERE), *a], capture_output=True, text=True, timeout=120)
    if check and r.returncode:
        raise RuntimeError(f"git {' '.join(a)}: {r.stderr.strip()[:400]}")
    return r.stdout.strip()


def is_data_commit(rev: str) -> bool:
    return git("log", "-1", "--format=%s%x1f%an", rev) == f"{DATA_MSG}\x1f{PUSHER_NAME}"


def commit(msg: str, path: str) -> None:
    git("add", path)
    git("-c", f"user.name={PUSHER_NAME}", "-c", f"user.email={PUSHER_EMAIL}",
        "commit", "-q", "--allow-empty", "-m", msg, "--", path)


def push_data(R: str) -> None:
    base = git("rev-parse", f"{R}~1") if is_data_commit(R) else R
    git("reset", "-q", "--mixed", base)
    commit(DATA_MSG, "data.json")
    git("push", "-q", f"--force-with-lease=refs/heads/main:{R}", "origin", "HEAD:main")


def write_data() -> dict:
    d = build()
    tmp = HERE / "data.json.tmp"
    with open(tmp, "w") as fh:
        json.dump(d, fh, separators=(",", ":"))
    os.replace(tmp, HERE / "data.json")
    return d


def last_hash() -> str | None:
    try:
        return HASH_FILE.read_text().strip() or None
    except OSError:
        return None


def save_hash(value: str) -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    tmp = str(HASH_FILE) + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(value + "\n")
    os.replace(tmp, HASH_FILE)


def main() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    once = "--once" in sys.argv
    nopush = "--no-push" in sys.argv
    last_push = 0.0
    fails = 0
    while True:
        try:
            if time.time() - last_push >= PUSH_EVERY_S or once or last_push == 0:
                if not nopush:
                    git("fetch", "-q", "origin", "main")
                    R = git("rev-parse", "refs/remotes/origin/main")
                else:
                    R = None
                d = write_data()
                digest = payload_hash(d)
                changed = digest != last_hash()
                alive = d["bot"]["alive"]
                n_alerts = len(d["alerts"])
                adx_new = sum(1 for r in d["adx"]["rows"] if r.get("new_alert"))
                if not changed:
                    last_push = time.time()
                    fails = 0
                    log(f"unchanged alive={alive} mode={d['bot']['mode']} alerts={n_alerts} adx_new={adx_new}")
                else:
                    if not nopush:
                        push_data(R)
                    save_hash(digest)
                    last_push = time.time()
                    fails = 0
                    log(f"pushed alive={alive} mode={d['bot']['mode']} alerts={n_alerts} adx_new={adx_new}")
        except Exception as e:
            fails += 1
            last_push = time.time() - PUSH_EVERY_S + 30 * min(fails, 4)
            log("ERROR", repr(e))
            traceback.print_exc()
        if once:
            break
        time.sleep(15)


if __name__ == "__main__":
    main()
