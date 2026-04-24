# core/rate_limiter.py
# -*- coding: utf-8 -*-
"""提供基于令牌桶的 API 请求限流器。"""

import threading
import time


# ================================
# 速率限制器
# ================================
class RateLimiter:
    """按照设定速率限制并发任务的请求节奏。"""

    # ================================
    # 初始化限流器
    # ================================
    def __init__(self, rate):

        # 每秒允许生成的令牌数
        self.rate = rate
        self.tokens = rate
        self.timestamp = time.time()
        self.lock = threading.Lock()

    # ================================
    # 获取令牌
    # ================================
    def acquire(self, tokens=1):

        while True:
            with self.lock:
                now = time.time()
                delta = now - self.timestamp
                self.timestamp = now

                self.tokens += delta * self.rate
                if self.tokens > self.rate:
                    self.tokens = self.rate

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

            time.sleep(0.01)
