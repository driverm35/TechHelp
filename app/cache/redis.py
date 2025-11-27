from __future__ import annotations
from typing import Any

import json
import redis.asyncio as redis
from app.config import settings

_redis: redis.Redis | None = None

def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, decode_responses=True)
    return _redis

async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    r = get_redis()
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    await r.set(key, value, ex=ttl)

async def cache_get(key: str) -> Any | None:
    r = get_redis()
    val = await r.get(key)
    if val is None:
        return None
    try:
        return json.loads(val)
    except Exception:
        return val

class RedisLock:
    def __init__(self, name: str, ttl: int = 60):
        self.name = f"lock:{name}"
        self.ttl = ttl
        self.r = get_redis()

    async def __aenter__(self):
        ok = await self.r.set(self.name, "1", nx=True, ex=self.ttl)
        if not ok:
            raise RuntimeError("Lock already held")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self.r.delete(self.name)
        except Exception:
            pass
