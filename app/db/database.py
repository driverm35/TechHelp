# app/db/database.py
from __future__ import annotations

import logging
import time

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import event, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool

from app.config import settings

logger = logging.getLogger(__name__)


# =========================
# Declarative Base
# =========================
class Base(DeclarativeBase):
    pass


# =========================
# Пул соединений
# =========================
is_sqlite = settings.db_dsn.startswith("sqlite")
if is_sqlite:
    poolclass = NullPool
    pool_kwargs: dict = {}
    connect_args: dict = {}  # для sqlite не нужно
else:
    poolclass = AsyncAdaptedQueuePool
    pool_kwargs = {
        "pool_size": 20,
        "max_overflow": 30,
        "pool_timeout": 30,
        "pool_recycle": 3600,
        "pool_pre_ping": True,
        "pool_reset_on_return": "rollback",
    }
    # Безопасные server_settings для Postgres (asyncpg)
    connect_args = {
        "server_settings": {
            "application_name": "supportbot",
            "statement_timeout": "60000",                     # 60s
            "idle_in_transaction_session_timeout": "300000",  # 5m
        },
        "command_timeout": 60,
        "timeout": 10,
    }

# =========================
# Engine
# =========================

# Общие execution_options
execution_options = {"compiled_cache_size": 500}
# Для Postgres/других СУБД можно указать READ COMMITTED,
# для sqlite — нельзя, поэтому не трогаем.
if not is_sqlite:
    execution_options["isolation_level"] = "READ COMMITTED"

engine: AsyncEngine = create_async_engine(
    settings.db_dsn,
    echo=settings.is_dev,  # подробнее в dev
    future=True,
    poolclass=poolclass,
    execution_options=execution_options,
    connect_args=connect_args,
    **pool_kwargs,
)

# =========================
# Session factory
# =========================
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,   # заметный буст производительности и меньше сюрпризов
    autocommit=False,
)

# ============================================================================
# ADVANCED SESSION MANAGER WITH READ REPLICAS
# ============================================================================

class DatabaseManager:
    """Продвинутый менеджер БД с поддержкой реплик и кеширования"""

    def __init__(self):
        self.engine = engine
        self.read_replica_engine: Optional[AsyncEngine] = None

        if hasattr(settings, 'DATABASE_READ_REPLICA_URL') and settings.DATABASE_READ_REPLICA_URL:
            self.read_replica_engine = create_async_engine(
                settings.DATABASE_READ_REPLICA_URL,
                poolclass=poolclass,
                pool_size=30,  # Больше для read операций
                max_overflow=50,
                pool_pre_ping=True,
                echo=False,
            )

    @asynccontextmanager
    async def session(self, read_only: bool = False):
        target_engine = self.read_replica_engine if (read_only and self.read_replica_engine) else self.engine

        async_session = async_sessionmaker(
            bind=target_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

        async with async_session() as session:
            try:
                yield session
                if not read_only:
                    await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def health_check(self) -> dict:
        pool = self.engine.pool

        try:
            async with AsyncSessionLocal() as session:
                start = time.time()
                await session.execute(text("SELECT 1"))
                latency = (time.time() - start) * 1000
            status = "healthy"
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            status = "unhealthy"
            latency = None

        return {
            "status": status,
            "latency_ms": round(latency, 2) if latency else None,
            "pool": _collect_health_pool_metrics(pool),
        }

db_manager = DatabaseManager()

# =========================
# DI / dependency
# =========================
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Единая точка получения сессии (FastAPI/Aiogram)."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            # Коммит контролируем в бизнес-слое, поэтому здесь ничего не делаем
            # (чтобы не коммитить «случайно»).
            pass


# ============================================================================
# INITIALIZATION AND CLEANUP
# ============================================================================

async def init_db():
    """Инициализация БД с оптимизациями"""
    logger.info("🚀 Создание таблиц базы данных...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    if not settings.db_dsn.startswith("sqlite"):
        logger.info("📊 Создание индексов для оптимизации...")

        async with engine.begin() as conn:
            indexes = [
                ("users", "CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)")
            ]

            for table_name, index_sql in indexes:
                table_exists = await conn.run_sync(lambda sync_conn: inspect(sync_conn).has_table(table_name))

                if not table_exists:
                    logger.debug(
                        "Пропускаем создание индекса %s: таблица %s отсутствует",
                        index_sql,
                        table_name,
                    )
                    continue

                try:
                    await conn.execute(text(index_sql))
                except Exception as e:
                    logger.debug("Index creation skipped for %s: %s", table_name, e)

    logger.info("✅ База данных успешно инициализирована")

    health = await db_manager.health_check()
    logger.info(f"📊 Database health: {health}")

async def close_db():
    """Корректное закрытие всех соединений"""
    logger.info("🔄 Закрытие соединений с БД...")

    await engine.dispose()

    if db_manager.read_replica_engine:
        await db_manager.read_replica_engine.dispose()

    logger.info("✅ Все подключения к базе данных закрыты")

# ============================================================================
# CONNECTION POOL METRICS (для мониторинга)
# ============================================================================

def _pool_counters(pool):
    """Return basic pool counters or ``None`` when unsupported."""

    required_methods = ("size", "checkedin", "checkedout", "overflow")

    for method_name in required_methods:
        method = getattr(pool, method_name, None)
        if method is None or not callable(method):
            return None

    size = pool.size()
    checked_in = pool.checkedin()
    checked_out = pool.checkedout()
    overflow = pool.overflow()

    total_connections = size + overflow

    return {
        "size": size,
        "checked_in": checked_in,
        "checked_out": checked_out,
        "overflow": overflow,
        "total_connections": total_connections,
        "utilization_percent": (checked_out / total_connections * 100) if total_connections else 0.0,
    }


def _collect_health_pool_metrics(pool) -> dict:
    counters = _pool_counters(pool)

    if counters is None:
        return {
            "metrics_available": False,
            "size": 0,
            "checked_in": 0,
            "checked_out": 0,
            "overflow": 0,
            "total_connections": 0,
            "utilization": "0.0%",
        }

    return {
        "metrics_available": True,
        "size": counters["size"],
        "checked_in": counters["checked_in"],
        "checked_out": counters["checked_out"],
        "overflow": counters["overflow"],
        "total_connections": counters["total_connections"],
        "utilization": f"{counters['utilization_percent']:.1f}%",
    }


async def get_pool_metrics() -> dict:
    """Детальные метрики пула для Prometheus/Grafana"""
    pool = engine.pool

    counters = _pool_counters(pool)

    if counters is None:
        return {
            "metrics_available": False,
            "pool_size": 0,
            "checked_in_connections": 0,
            "checked_out_connections": 0,
            "overflow_connections": 0,
            "total_connections": 0,
            "max_possible_connections": 0,
            "pool_utilization_percent": 0.0,
        }

    return {
        "metrics_available": True,
        "pool_size": counters["size"],
        "checked_in_connections": counters["checked_in"],
        "checked_out_connections": counters["checked_out"],
        "overflow_connections": counters["overflow"],
        "total_connections": counters["total_connections"],
        "max_possible_connections": counters["total_connections"] + (getattr(pool, "_max_overflow", 0) or 0),
        "pool_utilization_percent": round(counters["utilization_percent"], 2),
    }

# =========================
# Debug: медленные запросы
# =========================
if settings.is_dev:
    # В dev логируем длительность выполнения, чтобы быстро видеть "узкие места".
    from sqlalchemy import event

    @event.listens_for(Engine, "before_cursor_execute")
    def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault("query_start_time", []).append(time.perf_counter())

    @event.listens_for(Engine, "after_cursor_execute")
    def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start_list = conn.info.get("query_start_time")
        if not start_list:
            return
        total = time.perf_counter() - start_list.pop(-1)
        # Порог — 100ms. Всё, что дольше, подсвечиваем как slow.
        if total > 0.1:
            logger.warning("🐌 Slow query (%.3fs): %s", total, statement[:120])
        else:
            logger.debug("⚡ Query in %.3fs", total)
