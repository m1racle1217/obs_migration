# -*- coding: utf-8 -*-
"""构建并缓存目标端对象索引，用于加速存在性判断。"""

import logging

try:
    from obs import ObsClient
except ImportError:
    class ObsClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("obs sdk is required for remote storage operations")

from .retry import call_with_retries


# ================================
# 构建对象索引
# ================================
def build_obs_index(
    ak,
    sk,
    endpoint,
    bucket,
    prefix,
    checkpoint,
    stop_event=None,
    low_level_retries=3,
    low_level_retry_sleep=0.5,
    request_timeout=60,
):
    client = ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=endpoint,
        timeout=max(int(request_timeout or 60), 1),
    )

    marker = None
    total = 0
    prefix = prefix or ""

    logging.info("[OBS_INDEX] start build index prefix=%s", prefix)

    while True:
        if stop_event is not None and stop_event.is_set():
            logging.warning("[OBS_INDEX] stopped before completion total=%s", total)
            return

        response = call_with_retries(
            lambda: client.listObjects(
                bucket,
                prefix=prefix,
                marker=marker,
                max_keys=1000,
            ),
            retries=low_level_retries,
            base_sleep=low_level_retry_sleep,
            operation=f"indexList:{prefix or '/'}",
            logger=logging.getLogger(__name__),
        )

        if response.status >= 300:
            raise RuntimeError(f"OBS list error {response.status}")

        rows = [
            (obj.key, obj.size, getattr(obj, "etag", None))
            for obj in getattr(response.body, "contents", []) or []
        ]
        checkpoint.upsert_obs_many(rows)
        total += len(rows)

        if total and total % 10000 == 0:
            logging.info("[OBS_INDEX] cached %s objects", total)

        if not response.body.is_truncated:
            break

        marker = response.body.next_marker

    checkpoint.set_index_ready()
    logging.info("[OBS_INDEX] done total=%s", total)
