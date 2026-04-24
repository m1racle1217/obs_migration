# -*- coding: utf-8 -*-
"""提供基于令牌桶的轻量级 API 限流能力。"""

import threading
import time


# ================================
# 令牌桶限流器
# ================================
class RateLimiter:
    """按照设定速率与突发容量限制请求节奏。"""

    # ================================
    # 初始化限流器
    # ================================
    def __init__(self, rate, burst=None):
        self.rate = max(float(rate or 0), 0.0)
        self.burst = max(float(burst if burst is not None else self.rate or 1.0), 1.0)
        self.tokens = self.burst
        self.timestamp = time.time()
        self.lock = threading.Lock()

    # ================================
    # 获取令牌
    # ================================
    def acquire(self, tokens=1):
        tokens = max(float(tokens or 0), 0.0)
        if tokens <= 0 or self.rate <= 0:
            return

        while True:
            with self.lock:
                now = time.time()
                delta = max(now - self.timestamp, 0.0)
                self.timestamp = now

                self.tokens = min(self.burst, self.tokens + delta * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                missing = tokens - self.tokens

            time.sleep(max(missing / self.rate, 0.001))
