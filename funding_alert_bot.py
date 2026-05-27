import asyncio
import html
import json
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes


load_dotenv()

# ============================================================
# SETTINGS
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

ADMIN_IDS = {
    int(x)
    for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",")
    if x.isdigit()
}

CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config.json"))

# Проверка актуальных интервалов каждые 15 минут
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "900"))

# Cooldown отдельно для каждого тикера
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "3600"))  # 1h

REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))

QUOTE = os.getenv("QUOTE", "USDT").upper()

# Minimum absolute funding rate on at least one exchange to consider alert.
# Example: 0.001 = 0.1%
MIN_FUNDING_RATE = Decimal(os.getenv("MIN_FUNDING_RATE", "0.001"))

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
MEXC_BASE_URL = os.getenv("MEXC_BASE_URL", "https://api.mexc.com").rstrip("/")

# MEXC funding_rate лимитный, поэтому обновляем ВСЕ common интервалы каждый цикл,
# но строго батчами, чтобы не ловить success=false / unknown.
MEXC_RATE_LIMIT_BATCH = int(os.getenv("MEXC_RATE_LIMIT_BATCH", "16"))
MEXC_RATE_LIMIT_SLEEP = float(os.getenv("MEXC_RATE_LIMIT_SLEEP", "2.1"))
MEXC_DETAIL_CONCURRENCY = int(os.getenv("MEXC_DETAIL_CONCURRENCY", "8"))

# Cache используется только как fallback, если MEXC endpoint временно не ответил.
USE_CACHE_FALLBACK = os.getenv("USE_CACHE_FALLBACK", "true").lower() in {"1", "true", "yes", "on"}

# Debug logs
LOG_INTERVALS = os.getenv("LOG_INTERVALS", "true").lower() in {"1", "true", "yes", "on"}
LOG_INTERVAL_LIMIT = int(os.getenv("LOG_INTERVAL_LIMIT", "300"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("funding-interval-bot")


# ============================================================
# STORAGE
# ============================================================

@dataclass
class IntervalCacheItem:
    interval_hours: int
    updated_at: float


@dataclass
class ScanStats:
    last_check_at: float | None = None
    binance_tokens: int = 0
    mexc_tokens: int = 0
    common_tokens: int = 0
    mexc_interval_missing: int = 0
    interval_mismatches: int = 0
    alert_candidates: int = 0
    alerts_sent: int = 0
    skipped_by_cooldown: int = 0
    error: str | None = None


@dataclass
class BotConfig:
    target_chat_id: int | None = None
    target_thread_id: int | None = None
    blacklist: set[str] = field(default_factory=set)

    # symbol -> last alert unix timestamp
    alert_cooldowns: dict[str, float] = field(default_factory=dict)

    # MEXC fallback cache: normalized symbol -> interval
    mexc_interval_cache: dict[str, IntervalCacheItem] = field(default_factory=dict)

    # last scan stats for /status
    last_scan: ScanStats = field(default_factory=ScanStats)


def load_config() -> BotConfig:
    if not CONFIG_PATH.exists():
        return BotConfig()

    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Cannot read config.json, using empty config")
        return BotConfig()

    cache: dict[str, IntervalCacheItem] = {}

    for symbol, item in raw.get("mexc_interval_cache", {}).items():
        try:
            cache[str(symbol).upper()] = IntervalCacheItem(
                interval_hours=int(item["interval_hours"]),
                updated_at=float(item["updated_at"]),
            )
        except Exception:
            continue

    last_raw = raw.get("last_scan", {}) if isinstance(raw.get("last_scan", {}), dict) else {}

    return BotConfig(
        target_chat_id=raw.get("target_chat_id"),
        target_thread_id=raw.get("target_thread_id"),
        blacklist=set(str(x).upper() for x in raw.get("blacklist", [])),
        alert_cooldowns={
            str(k).upper(): float(v)
            for k, v in raw.get("alert_cooldowns", {}).items()
        },
        mexc_interval_cache=cache,
        last_scan=ScanStats(
            last_check_at=last_raw.get("last_check_at"),
            binance_tokens=int(last_raw.get("binance_tokens", 0)),
            mexc_tokens=int(last_raw.get("mexc_tokens", 0)),
            common_tokens=int(last_raw.get("common_tokens", 0)),
            mexc_interval_missing=int(last_raw.get("mexc_interval_missing", 0)),
            interval_mismatches=int(last_raw.get("interval_mismatches", 0)),
            alert_candidates=int(last_raw.get("alert_candidates", 0)),
            alerts_sent=int(last_raw.get("alerts_sent", 0)),
            skipped_by_cooldown=int(last_raw.get("skipped_by_cooldown", 0)),
            error=last_raw.get("error"),
        ),
    )


config = load_config()


def save_config() -> None:
    data = {
        "target_chat_id": config.target_chat_id,
        "target_thread_id": config.target_thread_id,
        "blacklist": sorted(config.blacklist),
        "alert_cooldowns": config.alert_cooldowns,
        "mexc_interval_cache": {
            symbol: {
                "interval_hours": item.interval_hours,
                "updated_at": item.updated_at,
            }
            for symbol, item in sorted(config.mexc_interval_cache.items())
        },
        "last_scan": {
            "last_check_at": config.last_scan.last_check_at,
            "binance_tokens": config.last_scan.binance_tokens,
            "mexc_tokens": config.last_scan.mexc_tokens,
            "common_tokens": config.last_scan.common_tokens,
            "mexc_interval_missing": config.last_scan.mexc_interval_missing,
            "interval_mismatches": config.last_scan.interval_mismatches,
            "alert_candidates": config.last_scan.alert_candidates,
            "alerts_sent": config.last_scan.alerts_sent,
            "skipped_by_cooldown": config.last_scan.skipped_by_cooldown,
            "error": config.last_scan.error,
        },
    }

    CONFIG_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ============================================================
# MODELS
# ============================================================

@dataclass(frozen=True)
class FundingInfo:
    symbol: str              # normalized: BTCUSDT
    exchange_symbol: str     # Binance: BTCUSDT / MEXC: BTC_USDT
    rate: Decimal | None
    interval_hours: int | None
    next_funding_ms: int | None = None
    interval_source: str = "unknown"  # api / cache / default / unknown


@dataclass(frozen=True)
class IntervalMismatch:
    symbol: str
    binance: FundingInfo
    mexc: FundingInfo


# ============================================================
# HELPERS
# ============================================================

def is_admin(user_id: int | None) -> bool:
    # Empty ADMIN_IDS = allow everyone. Handy for local testing.
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


def normalize_symbol(symbol: str) -> str:
    return symbol.replace("_", "").replace("-", "").upper()


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_mexc_interval_hours(item: dict[str, Any]) -> int | None:
    """
    MEXC interval field names differ across endpoints.
    Most useful field:
      collectCycle = 1 / 4 / 8 hours
    Also supports seconds/ms variants defensively.
    """
    candidates = [
        item.get("collectCycle"),
        item.get("fundingIntervalHours"),
        item.get("fundingCycle"),
        item.get("fundingInterval"),
        item.get("interval"),
    ]

    for value in candidates:
        n = parse_int(value)

        if n is None:
            continue

        # already hours
        if 1 <= n <= 24:
            return n

        # seconds -> hours
        if n in {3600, 7200, 14400, 28800}:
            return n // 3600

        # milliseconds -> hours
        if n in {3600000, 7200000, 14400000, 28800000}:
            return n // 3600000

    return None


def fmt_rate(rate: Decimal | None) -> str:
    if rate is None:
        return "unknown"

    return f"{rate * Decimal('100'):.4f}%"


def fmt_interval(hours: int | None) -> str:
    return "unknown" if hours is None else f"{hours}h"


def fmt_local_time(timestamp: float | None) -> str:
    if not timestamp:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def esc(value: Any) -> str:
    return html.escape(str(value))


def cooldown_left(symbol: str) -> int:
    last_ts = config.alert_cooldowns.get(symbol)

    if not last_ts:
        return 0

    left = int(ALERT_COOLDOWN_SECONDS - (time.time() - last_ts))
    return max(0, left)


def cleanup_old_cooldowns(active_symbols: set[str]) -> None:
    now = time.time()

    for symbol, last_ts in list(config.alert_cooldowns.items()):
        too_old = now - last_ts > ALERT_COOLDOWN_SECONDS * 3
        inactive = symbol not in active_symbols

        if too_old or inactive:
            del config.alert_cooldowns[symbol]


def cache_get_mexc_interval(symbol: str) -> int | None:
    item = config.mexc_interval_cache.get(symbol)

    if item is None:
        return None

    return item.interval_hours


def cache_set_mexc_interval(symbol: str, interval_hours: int) -> None:
    config.mexc_interval_cache[symbol] = IntervalCacheItem(
        interval_hours=interval_hours,
        updated_at=time.time(),
    )


# ============================================================
# HTTP
# ============================================================

def make_connector() -> aiohttp.TCPConnector:
    # Fix for Windows + aiodns: force system DNS resolver.
    return aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(),
        family=socket.AF_INET,
        ttl_dns_cache=300,
        limit=50,
    )


async def request_json(session: aiohttp.ClientSession, url: str) -> Any:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    async with session.get(url, timeout=timeout) as response:
        text = await response.text()

        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} {url}: {text[:300]}")

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Bad JSON from {url}: {text[:300]}") from exc

        # MEXC can return HTTP 200 with success=false
        if isinstance(payload, dict) and payload.get("success") is False:
            raise RuntimeError(f"API success=false {url}: {str(payload)[:300]}")

        return payload


# ============================================================
# BINANCE
# ============================================================

async def fetch_binance(session: aiohttp.ClientSession) -> dict[str, FundingInfo]:
    """
    Binance intervals are refreshed every scan.
    /fundingInfo gives non-default intervals.
    If symbol is absent there, Binance default is treated as 8h.
    """
    premium_url = f"{BINANCE_BASE_URL}/fapi/v1/premiumIndex"
    funding_info_url = f"{BINANCE_BASE_URL}/fapi/v1/fundingInfo"

    premium_data, funding_info_data = await asyncio.gather(
        request_json(session, premium_url),
        request_json(session, funding_info_url),
        return_exceptions=True,
    )

    if isinstance(premium_data, Exception):
        raise RuntimeError(f"Binance premiumIndex failed: {premium_data}") from premium_data

    interval_map: dict[str, int] = {}

    if isinstance(funding_info_data, Exception):
        logger.warning("Binance fundingInfo failed, using 8h default: %s", funding_info_data)
    else:
        for item in funding_info_data:
            symbol = str(item.get("symbol", "")).upper()
            interval = parse_int(item.get("fundingIntervalHours"))

            if symbol and interval:
                interval_map[symbol] = interval

    result: dict[str, FundingInfo] = {}

    for item in premium_data:
        symbol = str(item.get("symbol", "")).upper()

        if not symbol.endswith(QUOTE):
            continue

        interval = interval_map.get(symbol, 8)

        result[symbol] = FundingInfo(
            symbol=symbol,
            exchange_symbol=symbol,
            rate=parse_decimal(item.get("lastFundingRate")),
            interval_hours=interval,
            next_funding_ms=parse_int(item.get("nextFundingTime")),
            interval_source="api" if symbol in interval_map else "default",
        )

    return result


# ============================================================
# MEXC
# ============================================================

async def fetch_mexc_tickers(session: aiohttp.ClientSession) -> dict[str, FundingInfo]:
    """
    Cheap one-call snapshot:
    - all MEXC futures symbols
    - current funding rates
    - next funding time sometimes
    Intervals are filled by refresh_mexc_intervals_every_scan().
    """
    ticker_url = f"{MEXC_BASE_URL}/api/v1/contract/ticker"
    ticker_data = await request_json(session, ticker_url)
    tickers = ticker_data.get("data", []) if isinstance(ticker_data, dict) else []

    result: dict[str, FundingInfo] = {}

    for item in tickers:
        raw_symbol = str(item.get("symbol", "")).upper()
        symbol = normalize_symbol(raw_symbol)

        if not symbol.endswith(QUOTE):
            continue

        result[symbol] = FundingInfo(
            symbol=symbol,
            exchange_symbol=raw_symbol,
            rate=parse_decimal(item.get("fundingRate")),
            interval_hours=None,
            next_funding_ms=parse_int(item.get("nextSettleTime")),
            interval_source="unknown",
        )

    return result


async def fetch_mexc_interval_detail(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    base: FundingInfo,
) -> tuple[str, int | None, str]:
    """
    Returns (normalized symbol, interval_hours, source).
    Current interval is requested every scan.
    Cache is NOT used here except outside as fallback if this fails.
    """
    async with sem:
        detail_url = f"{MEXC_BASE_URL}/api/v1/contract/funding_rate/{base.exchange_symbol}"

        try:
            payload = await request_json(session, detail_url)
            item = payload.get("data", {}) if isinstance(payload, dict) else {}

            if isinstance(item, dict):
                interval = parse_mexc_interval_hours(item)

                if interval is not None:
                    return base.symbol, interval, "api"

        except Exception as exc:
            logger.warning("MEXC funding_rate failed | %s | %s", base.exchange_symbol, exc)

        params = urlencode({
            "symbol": base.exchange_symbol,
            "page_num": 1,
            "page_size": 1,
        })

        history_url = f"{MEXC_BASE_URL}/api/v1/contract/funding_rate/history?{params}"

        try:
            payload = await request_json(session, history_url)
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            rows = data.get("resultList", []) if isinstance(data, dict) else []

            if rows and isinstance(rows[0], dict):
                interval = parse_mexc_interval_hours(rows[0])

                if interval is not None:
                    return base.symbol, interval, "history"

        except Exception as exc:
            logger.warning("MEXC funding history failed | %s | %s", base.exchange_symbol, exc)

        return base.symbol, None, "unknown"


async def refresh_mexc_intervals_every_scan(
    session: aiohttp.ClientSession,
    mexc: dict[str, FundingInfo],
    common_symbols: list[str],
) -> dict[str, FundingInfo]:
    """
    Correct logic for this project:
    - every 15 minutes refresh actual MEXC intervals for all common symbols
    - do it in safe batches because MEXC has strict rate limits
    - cache is fallback only, not primary source
    """
    sem = asyncio.Semaphore(MEXC_DETAIL_CONCURRENCY)
    enriched = dict(mexc)

    bases = [mexc[symbol] for symbol in common_symbols if symbol in mexc]

    logger.info(
        "MEXC INTERVAL REFRESH EVERY SCAN | common=%s | batch=%s | sleep=%ss | concurrency=%s | cache_fallback=%s",
        len(bases),
        MEXC_RATE_LIMIT_BATCH,
        MEXC_RATE_LIMIT_SLEEP,
        MEXC_DETAIL_CONCURRENCY,
        USE_CACHE_FALLBACK,
    )

    api_ok = 0
    history_ok = 0
    cache_fallback = 0
    unknown = 0

    for start in range(0, len(bases), MEXC_RATE_LIMIT_BATCH):
        batch = bases[start:start + MEXC_RATE_LIMIT_BATCH]

        logger.info(
            "MEXC INTERVAL BATCH | %s-%s / %s",
            start + 1,
            start + len(batch),
            len(bases),
        )

        results = await asyncio.gather(
            *(fetch_mexc_interval_detail(session, sem, base) for base in batch)
        )

        for symbol, interval, source in results:
            base = mexc[symbol]

            if interval is not None:
                cache_set_mexc_interval(symbol, interval)

                if source == "api":
                    api_ok += 1
                elif source == "history":
                    history_ok += 1

                enriched[symbol] = FundingInfo(
                    symbol=base.symbol,
                    exchange_symbol=base.exchange_symbol,
                    rate=base.rate,
                    interval_hours=interval,
                    next_funding_ms=base.next_funding_ms,
                    interval_source=source,
                )

                continue

            cached = cache_get_mexc_interval(symbol)

            if USE_CACHE_FALLBACK and cached is not None:
                cache_fallback += 1

                enriched[symbol] = FundingInfo(
                    symbol=base.symbol,
                    exchange_symbol=base.exchange_symbol,
                    rate=base.rate,
                    interval_hours=cached,
                    next_funding_ms=base.next_funding_ms,
                    interval_source="cache",
                )
            else:
                unknown += 1

                enriched[symbol] = FundingInfo(
                    symbol=base.symbol,
                    exchange_symbol=base.exchange_symbol,
                    rate=base.rate,
                    interval_hours=None,
                    next_funding_ms=base.next_funding_ms,
                    interval_source="unknown",
                )

        save_config()

        if start + MEXC_RATE_LIMIT_BATCH < len(bases):
            await asyncio.sleep(MEXC_RATE_LIMIT_SLEEP)

    logger.info(
        "MEXC INTERVAL RESULT | api=%s | history=%s | cache_fallback=%s | unknown=%s | cache_size=%s",
        api_ok,
        history_ok,
        cache_fallback,
        unknown,
        len(config.mexc_interval_cache),
    )

    return enriched


# ============================================================
# CORE LOGIC
# ============================================================

async def scan() -> tuple[
    dict[str, FundingInfo],
    dict[str, FundingInfo],
    list[str],
    list[IntervalMismatch],
]:
    headers = {"User-Agent": "funding-interval-bot-current-intervals-v1"}
    connector = make_connector()

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        binance, mexc = await asyncio.gather(
            fetch_binance(session),
            fetch_mexc_tickers(session),
        )

        common = sorted(set(binance) & set(mexc))

        mexc = await refresh_mexc_intervals_every_scan(
            session=session,
            mexc=mexc,
            common_symbols=common,
        )

    mismatches: list[IntervalMismatch] = []
    missing_mexc_intervals = 0

    for symbol in common:
        if symbol in config.blacklist:
            continue

        b = binance[symbol]
        m = mexc[symbol]

        if m.interval_hours is None:
            missing_mexc_intervals += 1
            continue

        if b.interval_hours is None:
            continue

        # Skip dead/near-zero funding pairs.
        # At least one side must have abs funding >= MIN_FUNDING_RATE.
        b_abs = abs(b.rate) if b.rate is not None else Decimal("0")
        m_abs = abs(m.rate) if m.rate is not None else Decimal("0")

        if max(b_abs, m_abs) < MIN_FUNDING_RATE:
            continue

        if b.interval_hours != m.interval_hours:
            mismatches.append(IntervalMismatch(symbol=symbol, binance=b, mexc=m))

    logger.info(
        "SCAN | Binance tokens=%s | MEXC tokens=%s | common=%s | interval mismatches=%s | MEXC interval missing=%s | blacklist=%s",
        len(binance),
        len(mexc),
        len(common),
        len(mismatches),
        missing_mexc_intervals,
        len(config.blacklist),
    )

    if common:
        logger.info("COMMON SAMPLE | %s", ", ".join(common[:40]))

    if LOG_INTERVALS:
        logger.info("========== COMMON FUNDING INTERVALS ==========")

        for symbol in common[:LOG_INTERVAL_LIMIT]:
            b = binance[symbol]
            m = mexc[symbol]

            logger.info(
                "%-18s | BINANCE=%-7s %-7s | MEXC=%-7s %-7s | MATCH=%s | B_RATE=%s | M_RATE=%s | M_RAW=%s",
                symbol,
                fmt_interval(b.interval_hours),
                f"({b.interval_source})",
                fmt_interval(m.interval_hours),
                f"({m.interval_source})",
                b.interval_hours == m.interval_hours,
                fmt_rate(b.rate),
                fmt_rate(m.rate),
                m.exchange_symbol,
            )

    if mismatches:
        logger.info(
            "ALERT CANDIDATES | %s",
            ", ".join(
                f"{x.symbol}(B={fmt_interval(x.binance.interval_hours)},M={fmt_interval(x.mexc.interval_hours)},src={x.mexc.interval_source})"
                for x in mismatches[:80]
            ),
        )

    return binance, mexc, common, mismatches


def render_alert(item: IntervalMismatch) -> str:
    return (
        "🚨 <b>Разный funding interval</b>\n\n"
        f"<b>{esc(item.symbol)}</b>\n\n"
        f"Binance: <b>{fmt_interval(item.binance.interval_hours)}</b> "
        f"({esc(item.binance.interval_source)}) | funding {fmt_rate(item.binance.rate)}\n"
        f"MEXC: <b>{fmt_interval(item.mexc.interval_hours)}</b> "
        f"({esc(item.mexc.interval_source)}) | funding {fmt_rate(item.mexc.rate)}\n\n"
        f"MEXC symbol: <code>{esc(item.mexc.exchange_symbol)}</code>\n"
        f"Blacklist: <code>/bladd {esc(item.symbol)}</code>"
    )


async def send_alert(context: ContextTypes.DEFAULT_TYPE, item: IntervalMismatch) -> None:
    if config.target_chat_id is None:
        logger.warning("Target chat is not bound. Send /bind in target chat/thread.")
        return

    await context.bot.send_message(
        chat_id=config.target_chat_id,
        message_thread_id=config.target_thread_id,
        text=render_alert(item),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        binance, mexc, common, mismatches = await scan()
    except Exception as exc:
        logger.exception("Check failed: %s", exc)
        config.last_scan = ScanStats(
            last_check_at=time.time(),
            error=str(exc),
        )
        save_config()
        return

    active_symbols = {x.symbol for x in mismatches}
    cleanup_old_cooldowns(active_symbols)

    to_send: list[IntervalMismatch] = []
    skipped_cd = 0

    for item in mismatches:
        if cooldown_left(item.symbol) > 0:
            skipped_cd += 1
            continue

        to_send.append(item)

    missing_mexc = sum(1 for s in common if mexc[s].interval_hours is None)

    config.last_scan = ScanStats(
        last_check_at=time.time(),
        binance_tokens=len(binance),
        mexc_tokens=len(mexc),
        common_tokens=len(common),
        mexc_interval_missing=missing_mexc,
        interval_mismatches=len(mismatches),
        alert_candidates=len(mismatches),
        alerts_sent=0,
        skipped_by_cooldown=skipped_cd,
        error=None,
    )

    logger.info(
        "SEND PLAN | candidates=%s | to_send=%s | skipped_by_cooldown=%s | target=%s thread=%s",
        len(mismatches),
        len(to_send),
        skipped_cd,
        config.target_chat_id,
        config.target_thread_id,
    )

    for item in to_send:
        try:
            await send_alert(context, item)
        except Exception:
            logger.exception("Failed to send alert for %s", item.symbol)
            continue

        config.alert_cooldowns[item.symbol] = time.time()
        config.last_scan.alerts_sent += 1
        logger.info("ALERT SENT | %s", item.symbol)

    save_config()


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Бот ловит монеты, которые есть на Binance и MEXC, "
        "но имеют разный funding interval.\n\n"
        "Команды:\n"
        "/bind — привязать текущий чат/ветку\n"
        "/check — ручная проверка с актуальными интервалами\n"
        "/bladd BTCUSDT — добавить в blacklist\n"
        "/bldel BTCUSDT — убрать из blacklist\n"
        "/bllist — показать blacklist\n"
        "/status — статус"
    )


async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return

    args = context.args

    if args:
        config.target_chat_id = int(args[0])
        config.target_thread_id = int(args[1]) if len(args) > 1 else None
    else:
        config.target_chat_id = update.effective_chat.id
        config.target_thread_id = update.effective_message.message_thread_id

    save_config()

    await update.message.reply_text(
        f"✅ Алерты привязаны.\n"
        f"chat_id: {config.target_chat_id}\n"
        f"thread_id: {config.target_thread_id}"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    last = config.last_scan

    await update.message.reply_text(
        f"Target chat: {config.target_chat_id}\n"
        f"Target thread: {config.target_thread_id}\n"
        f"Check interval: {CHECK_INTERVAL_SECONDS}s / {CHECK_INTERVAL_SECONDS // 60} min\n"
        f"Alert cooldown: {ALERT_COOLDOWN_SECONDS}s / {ALERT_COOLDOWN_SECONDS // 60} min\n"
        f"MEXC interval cache fallback: {USE_CACHE_FALLBACK}\n"
        f"MEXC interval cache size: {len(config.mexc_interval_cache)}\n"
        f"MEXC batch: {MEXC_RATE_LIMIT_BATCH}, sleep: {MEXC_RATE_LIMIT_SLEEP}s\n"
        f"Blacklist: {len(config.blacklist)} тикеров\n"
        f"Cooldown active: {len(config.alert_cooldowns)} тикеров\n"
        f"Log intervals: {LOG_INTERVALS}\n\n"
        f"Last check: {fmt_local_time(last.last_check_at)}\n"
        f"Last error: {last.error or 'none'}\n"
        f"Binance tokens: {last.binance_tokens}\n"
        f"MEXC tokens: {last.mexc_tokens}\n"
        f"Common tokens: {last.common_tokens}\n"
        f"MEXC interval missing: {last.mexc_interval_missing}\n"
        f"Interval mismatches: {last.interval_mismatches}\n"
        f"Alert candidates: {last.alert_candidates}\n"
        f"Alerts sent: {last.alerts_sent}\n"
        f"Skipped by cooldown: {last.skipped_by_cooldown}"
    )


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return

    msg = await update.message.reply_text("Проверяю актуальные интервалы Binance/MEXC...")

    try:
        binance, mexc, common, mismatches = await scan()
    except Exception as exc:
        logger.exception("Manual check failed")
        config.last_scan = ScanStats(
            last_check_at=time.time(),
            error=str(exc),
        )
        save_config()
        await msg.edit_text(f"❌ Ошибка проверки:\n{exc}")
        return

    to_send = [x for x in mismatches if cooldown_left(x.symbol) == 0]
    skipped_cd = len(mismatches) - len(to_send)
    missing_mexc = sum(1 for s in common if mexc[s].interval_hours is None)

    config.last_scan = ScanStats(
        last_check_at=time.time(),
        binance_tokens=len(binance),
        mexc_tokens=len(mexc),
        common_tokens=len(common),
        mexc_interval_missing=missing_mexc,
        interval_mismatches=len(mismatches),
        alert_candidates=len(mismatches),
        alerts_sent=0,
        skipped_by_cooldown=skipped_cd,
        error=None,
    )
    save_config()

    lines = [
        f"Binance tokens: {len(binance)}",
        f"MEXC tokens: {len(mexc)}",
        f"Common tokens: {len(common)}",
        f"MEXC interval missing: {missing_mexc}",
        f"Interval mismatches: {len(mismatches)}",
        f"Would send now: {len(to_send)}",
        "",
    ]

    if mismatches:
        lines.append("Mismatches:")
        lines.extend(
            f"{x.symbol}: Binance {fmt_interval(x.binance.interval_hours)} / "
            f"MEXC {fmt_interval(x.mexc.interval_hours)} ({x.mexc.interval_source})"
            for x in mismatches[:30]
        )

        if len(mismatches) > 30:
            lines.append(f"...и ещё {len(mismatches) - 30}")

    await msg.edit_text("\n".join(lines))


async def bladd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("Пример: /bladd BTCUSDT")
        return

    symbol = normalize_symbol(context.args[0])
    config.blacklist.add(symbol)
    config.alert_cooldowns.pop(symbol, None)
    save_config()

    await update.message.reply_text(f"✅ {symbol} добавлен в blacklist")


async def bldel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text("Пример: /bldel BTCUSDT")
        return

    symbol = normalize_symbol(context.args[0])
    config.blacklist.discard(symbol)
    save_config()

    await update.message.reply_text(f"✅ {symbol} удалён из blacklist")


async def bllist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not config.blacklist:
        await update.message.reply_text("Blacklist пуст.")
        return

    await update.message.reply_text("Blacklist:\n" + "\n".join(sorted(config.blacklist)))


# ============================================================
# APP
# ============================================================

def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN in .env")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("check", check_cmd))
    app.add_handler(CommandHandler("bladd", bladd_cmd))
    app.add_handler(CommandHandler("bldel", bldel_cmd))
    app.add_handler(CommandHandler("bllist", bllist_cmd))

    app.job_queue.run_repeating(
        monitor_job,
        interval=CHECK_INTERVAL_SECONDS,
        first=5,
        name="funding_interval_monitor",
    )

    return app


def main() -> None:
    app = build_app()

    logger.info(
        "Bot started | check_interval=%ss | alert_cooldown=%ss | min_funding=%s%% | quote=%s | mexc_base=%s | batch=%s | sleep=%ss | cache_fallback=%s | log_intervals=%s",
        CHECK_INTERVAL_SECONDS,
        ALERT_COOLDOWN_SECONDS,
        f"{MIN_FUNDING_RATE * Decimal('100')}",
        QUOTE,
        MEXC_BASE_URL,
        MEXC_RATE_LIMIT_BATCH,
        MEXC_RATE_LIMIT_SLEEP,
        USE_CACHE_FALLBACK,
        LOG_INTERVALS,
    )

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
