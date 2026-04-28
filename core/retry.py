# -*- coding: utf-8 -*-
"""提供面向网络请求的分层重试辅助函数。"""

import logging
import random
import time


RETRYABLE_STATUS = {
    408,
    409,
    425,
    429,
    500,
    502,
    503,
    504,
}


# ================================
# 判断状态码是否可重试
# ================================
def is_retryable_status(status):
    if status is None:
        return False
    try:
        status = int(status)
    except Exception:
        return False

    return status in RETRYABLE_STATUS


# ================================
# 执行低层网络重试
# ================================
def call_with_retries(
    func,
    retries=3,
    base_sleep=0.5,
    operation="request",
    heartbeat=None,
    logger=None,
):
    retries = max(0, int(retries or 0))
    base_sleep = max(float(base_sleep or 0.0), 0.0)
    logger = logger or logging.getLogger(__name__)

    attempt = 0
    last_response = None

    while True:
        if heartbeat is not None:
            try:
                heartbeat(operation)
            except Exception:
                pass

        try:
            response = func()
            last_response = response
        except Exception as exc:
            if attempt >= retries:
                raise

            delay = min(base_sleep * (2 ** attempt), 10.0) + random.random() * 0.2
            logger.warning(
                "[LOW_LEVEL_RETRY][EXCEPTION] op=%s attempt=%s/%s delay=%.2fs err=%s",
                operation,
                attempt + 1,
                retries,
                delay,
                exc,
            )
            time.sleep(delay)
            attempt += 1
            continue

        status = getattr(response, "status", None)
        if not is_retryable_status(status) or attempt >= retries:
            return response

        delay = min(base_sleep * (2 ** attempt), 10.0) + random.random() * 0.2
        logger.warning(
            "[LOW_LEVEL_RETRY][STATUS] op=%s attempt=%s/%s delay=%.2fs status=%s",
            operation,
            attempt + 1,
            retries,
            delay,
            status,
        )
        time.sleep(delay)
        attempt += 1

    return last_response