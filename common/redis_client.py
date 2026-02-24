from __future__ import annotations
import redis
from common.config import get_settings

_settings = get_settings()
_client = None

def get_redis_client():
    global _client
    if _client is None:
        _client = redis.Redis(host=_settings.redis_host, port=_settings.redis_port, decode_responses=True)
    return _client
