# core/obs_index.py
# -*- coding: utf-8 -*-
"""构建并缓存目标端对象索引，用于快速跳过已存在对象。"""

import logging

from obs import ObsClient


# ================================
# 构建对象索引
# ================================
def build_obs_index(ak, sk, endpoint, bucket, prefix, checkpoint):
    client = ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=endpoint,
    )

    marker = None
    total = 0
    prefix = prefix or ""

    logging.info(f"[OBS_INDEX] start build index prefix={prefix}")

    while True:
        resp = client.listObjects(
            bucket,
            prefix=prefix,
            marker=marker,
            max_keys=1000,
        )

        if resp.status >= 300:
            raise RuntimeError(f"OBS list error {resp.status}")

        rows = [
            (obj.key, obj.size, getattr(obj, "etag", None))
            for obj in getattr(resp.body, "contents", []) or []
        ]
        checkpoint.upsert_obs_many(rows)
        total += len(rows)

        if total and total % 10000 == 0:
            logging.info(f"[OBS_INDEX] cached {total} objects")

        if not resp.body.is_truncated:
            break

        marker = resp.body.next_marker

    checkpoint.set_index_ready()
    logging.info(f"[OBS_INDEX] done total={total}")
