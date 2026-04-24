# -*- coding: utf-8 -*-
"""提供统一的连接、限流与内存预算治理能力。"""

import threading
from contextlib import contextmanager

from .ratelimiter import RateLimiter


# ================================
# 统一资源治理器
# ================================
class ResourceGovernor:
    """统一限制连接数、API 速率和估算内存占用。"""

    # ================================
    # 初始化治理器
    # ================================
    def __init__(
        self,
        rate_limit=0,
        rate_limit_burst=None,
        max_connections=0,
        max_buffer_bytes=0,
    ):
        self.max_connections = max(0, int(max_connections or 0))
        self.max_buffer_bytes = max(0, int(max_buffer_bytes or 0))
        self.rate_limiter = None
        self._connection_semaphore = None
        self._buffer_condition = threading.Condition()
        self._reserved_buffer_bytes = 0

        if rate_limit and float(rate_limit) > 0:
            self.rate_limiter = RateLimiter(rate_limit, burst=rate_limit_burst)

        if self.max_connections > 0:
            self._connection_semaphore = threading.BoundedSemaphore(self.max_connections)

    # ================================
    # 获取 API 令牌
    # ================================
    def acquire_api(self, tokens=1):
        if self.rate_limiter is None:
            return
        self.rate_limiter.acquire(tokens=tokens)

    # ================================
    # 占用连接配额
    # ================================
    @contextmanager
    def connection_slot(self):
        if self._connection_semaphore is None:
            yield
            return

        self._connection_semaphore.acquire()
        try:
            yield
        finally:
            self._connection_semaphore.release()

    # ================================
    # 预留缓存预算
    # ================================
    @contextmanager
    def reserve_buffer(self, size):
        size = max(0, int(size or 0))
        if size <= 0 or self.max_buffer_bytes <= 0:
            yield
            return

        with self._buffer_condition:
            while (self._reserved_buffer_bytes + size) > self.max_buffer_bytes:
                self._buffer_condition.wait(timeout=0.2)
            self._reserved_buffer_bytes += size

        try:
            yield
        finally:
            with self._buffer_condition:
                self._reserved_buffer_bytes -= size
                if self._reserved_buffer_bytes < 0:
                    self._reserved_buffer_bytes = 0
                self._buffer_condition.notify_all()

    # ================================
    # 获取当前治理快照
    # ================================
    def snapshot(self):
        with self._buffer_condition:
            return {
                "max_connections": self.max_connections,
                "max_buffer_bytes": self.max_buffer_bytes,
                "reserved_buffer_bytes": self._reserved_buffer_bytes,
            }