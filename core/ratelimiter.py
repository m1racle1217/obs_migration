# core/rate_limiter.py
# -*- coding: utf-8 -*-

import time
import threading


class RateLimiter:

    def __init__(self, rate):

        # 每秒允许多少次
        self.rate = rate

        self.tokens = rate

        self.timestamp = time.time()

        self.lock = threading.Lock()

    # =============================

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