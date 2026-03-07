from __future__ import annotations

import asyncio
import logging

import structlog

from config import settings
from core.backtest_exporter import run_backtest_exporter
from core.binance_feed import start_binance_feed
from core.market_discovery import run_market_discovery
from core.models import BinanceTick
from core.orderbook_stream import TickEvent, stream_markets
from core.persistence import PersistenceWriter
from state import shared_state
from storage.db import get_engine, markets

log = structlog.get_logger()


def _configure_logging() -> None:
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
    )


def _restore_token_meta(engine: object) -> None:
    """Populate shared_state.token_meta from previously discovered markets in DB."""
    import sqlalchemy as sa
    with engine.connect() as conn:  # type: ignore[union-attr]
        for row in conn.execute(markets.select()).mappings():
            asset   = row.get("asset", "")
            horizon = row.get("horizon", "")
            yes_id  = row.get("token_id_yes", "")
            no_id   = row.get("token_id_no", "")
            if yes_id and asset and horizon:
                shared_state.token_meta[yes_id] = (asset, horizon)
            if no_id and asset and horizon:
                shared_state.token_meta[no_id] = (asset, horizon)
    log.info("startup.token_meta_restored", count=len(shared_state.token_meta))


async def _process_ticks(
    writer: PersistenceWriter,
    tick_queue: asyncio.Queue[TickEvent],
) -> None:
    while True:
        try:
            event = await tick_queue.get()
            writer.write_tick(event)
            tick_queue.task_done()
        except Exception as exc:
            log.error("tick_processor.error", error=str(exc))


async def _process_binance(
    writer: PersistenceWriter,
    queue: asyncio.Queue[BinanceTick],
) -> None:
    async def _consume() -> None:
        while True:
            try:
                tick = await queue.get()
                shared_state.binance_mids[tick.asset] = tick.mid
                writer.write_binance_tick(tick)
                queue.task_done()
            except Exception as exc:
                log.error("binance_processor.error", error=str(exc))

    async def _flush_loop() -> None:
        while True:
            await asyncio.sleep(settings.BINANCE_FLUSH_INTERVAL)
            try:
                writer.flush_binance()
            except Exception as exc:
                log.error("binance_flush.error", error=str(exc))

    await asyncio.gather(_consume(), _flush_loop())


async def _flush_loop(writer: PersistenceWriter) -> None:
    while True:
        await asyncio.sleep(settings.TICK_FLUSH_INTERVAL)
        try:
            writer.flush()
        except Exception as exc:
            log.error("flush_loop.error", error=str(exc))


async def main() -> None:
    _configure_logging()
    log.info("daemon.starting")

    engine = get_engine(settings.DB_PATH)
    _restore_token_meta(engine)

    tick_queue: asyncio.Queue[TickEvent] = asyncio.Queue(maxsize=50_000)
    binance_queue: asyncio.Queue[BinanceTick] = asyncio.Queue(maxsize=10_000)

    writer = PersistenceWriter()

    tasks = [
        asyncio.create_task(
            run_market_discovery(settings.ASSETS, settings.HORIZONS, engine,
                                 interval=settings.MARKET_DISCOVERY_INTERVAL),
            name="market_discovery",
        ),
        asyncio.create_task(
            stream_markets(tick_queue),
            name="orderbook_stream",
        ),
        asyncio.create_task(
            start_binance_feed(settings.ASSETS, binance_queue, settings.BINANCE_WS_URL),
            name="binance_feed",
        ),
        asyncio.create_task(
            _process_ticks(writer, tick_queue),
            name="tick_processor",
        ),
        asyncio.create_task(
            _process_binance(writer, binance_queue),
            name="binance_processor",
        ),
        asyncio.create_task(
            _flush_loop(writer),
            name="flush",
        ),
    ]

    if settings.BACKTEST_DATA_DIR:
        tasks.append(asyncio.create_task(
            run_backtest_exporter(
                backtest_data_dir=settings.BACKTEST_DATA_DIR,
                engine=engine,
                interval=settings.BACKTEST_EXPORT_INTERVAL,
                lookback_seconds=settings.BACKTEST_LOOKBACK_SECONDS,
            ),
            name="backtest_exporter",
        ))
        log.info("daemon.backtest_exporter_enabled", dir=settings.BACKTEST_DATA_DIR)

    log.info("daemon.ready", assets=settings.ASSETS, horizons=settings.HORIZONS)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("daemon.shutdown")
        writer.flush()
        writer.flush_binance()
    finally:
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
