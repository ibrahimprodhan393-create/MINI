from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import HTTPException

from .config import settings

_pool: asyncpg.Pool | None = None


async def connect_db() -> None:
    global _pool
    if _pool or not settings.database_url:
        return
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        statement_cache_size=0,
        command_timeout=30,
    )


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(
            status_code=503,
            detail="Database is not configured. Set DATABASE_URL in Render.",
        )
    return _pool


@asynccontextmanager
async def connection() -> AsyncIterator[asyncpg.Connection]:
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn
