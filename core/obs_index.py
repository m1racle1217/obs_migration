# -*- coding: utf-8 -*-
"""构建并缓存目标端对象索引，用于加速存在性判断。"""

import logging
import queue
import threading
from contextlib import nullcontext

try:
    from obs import ObsClient
except ImportError:
    class ObsClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("obs sdk is required for remote storage operations")

from .retry import call_with_retries
from .s3_scanner import _extract_common_prefixes
from .utils import normalize_obs_key, sanitize_key


# ================================
# 标准化前缀
# ================================
def _normalize_prefix(prefix):
    return sanitize_key(normalize_obs_key(prefix or "")).strip("/")


# ================================
# 执行带治理的对象列举
# ================================
def _list_objects(
    client,
    bucket,
    current_prefix,
    marker,
    low_level_retries=3,
    low_level_retry_sleep=0.5,
    governor=None,
):
    kwargs = {
        "prefix": current_prefix,
        "marker": marker,
        "max_keys": 1000,
    }

    def do_list():
        try:
            return client.listObjects(
                bucket,
                delimiter="/",
                **kwargs,
            )
        except TypeError:
            return client.listObjects(bucket, **kwargs)

    def governed_call():
        connection_context = governor.connection_slot() if governor is not None else nullcontext()
        with connection_context:
            if governor is not None:
                governor.acquire_api(1)
            return do_list()

    return call_with_retries(
        governed_call,
        retries=low_level_retries,
        base_sleep=low_level_retry_sleep,
        operation=f"indexList:{current_prefix or '/'}",
        logger=logging.getLogger(__name__),
    )


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
    workers=4,
    governor=None,
):
    def create_client():
        return ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
            timeout=max(int(request_timeout or 60), 1),
        )

    total = 0
    worker_count = max(1, int(workers or 1))
    root_prefix = _normalize_prefix(prefix)
    prefix_queue = queue.Queue()
    stop_token = object()
    total_lock = threading.Lock()
    seen_lock = threading.Lock()
    error_lock = threading.Lock()
    seen_prefixes = set()
    first_error = [None]
    inner_stop_event = threading.Event()

    logging.info(
        "[OBS_INDEX] start build index prefix=%s workers=%s",
        root_prefix,
        worker_count,
    )

    # ================================
    # 去重入队前缀
    # ================================
    def enqueue_prefix(current_prefix):
        normalized = sanitize_key(normalize_obs_key(current_prefix or ""))
        with seen_lock:
            if normalized in seen_prefixes:
                return
            seen_prefixes.add(normalized)
        prefix_queue.put(normalized)

    # ================================
    # 索引单个前缀
    # ================================
    def scan_prefix(client, current_prefix):
        nonlocal total
        marker = None

        while not inner_stop_event.is_set():
            if stop_event is not None and stop_event.is_set():
                inner_stop_event.set()
                return

            response = _list_objects(
                client,
                bucket,
                current_prefix,
                marker,
                low_level_retries=low_level_retries,
                low_level_retry_sleep=low_level_retry_sleep,
                governor=governor,
            )

            if response.status >= 300:
                raise RuntimeError(f"OBS list error {response.status}")

            body = getattr(response, "body", None)
            if body is None:
                break

            for child_prefix in _extract_common_prefixes(body):
                enqueue_prefix(child_prefix)

            rows = []
            for obj in getattr(body, "contents", []) or []:
                key = sanitize_key(normalize_obs_key(getattr(obj, "key", "") or "")).strip("/")
                if not key:
                    continue
                rows.append(
                    (
                        key,
                        int(getattr(obj, "size", 0) or 0),
                        getattr(obj, "etag", None),
                    )
                )

            checkpoint.upsert_obs_many(rows)

            with total_lock:
                total += len(rows)
                if total and total % 10000 == 0:
                    logging.info("[OBS_INDEX] cached %s objects", total)

            if not getattr(body, "is_truncated", False):
                break

            marker = getattr(body, "next_marker", None)

    # ================================
    # worker 主循环
    # ================================
    def worker():
        client = create_client()
        while True:
            current_prefix = prefix_queue.get()
            try:
                if current_prefix is stop_token:
                    return
                if inner_stop_event.is_set():
                    continue
                scan_prefix(client, current_prefix)
            except Exception as exc:
                inner_stop_event.set()
                with error_lock:
                    if first_error[0] is None:
                        first_error[0] = exc
                logging.exception("[OBS_INDEX][PREFIX_ERROR] prefix=%s", current_prefix)
            finally:
                prefix_queue.task_done()

    enqueue_prefix(root_prefix)
    threads = []
    for _ in range(worker_count):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        threads.append(thread)

    completed = True
    try:
        prefix_queue.join()
    finally:
        if stop_event is not None and stop_event.is_set():
            completed = False
        if inner_stop_event.is_set() and first_error[0] is None:
            completed = False
        for _ in range(worker_count):
            prefix_queue.put(stop_token)
        for thread in threads:
            thread.join()

    if first_error[0] is not None:
        checkpoint.flush_obs_index()
        raise first_error[0]

    if not completed:
        checkpoint.flush_obs_index()
        logging.warning("[OBS_INDEX] stopped before completion total=%s", total)
        return False

    checkpoint.set_index_ready()
    logging.info("[OBS_INDEX] done total=%s", total)
    return True
