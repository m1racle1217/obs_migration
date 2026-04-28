# -*- coding: utf-8 -*-
"""构建并缓存目标端对象索引，用于加速存在性判断。"""

import logging
import queue
import threading
from contextlib import nullcontext
from collections import deque

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
    delimiter=None,
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
        if delimiter is not None:
            try:
                return client.listObjects(
                    bucket,
                    delimiter=delimiter,
                    **kwargs,
                )
            except TypeError:
                return client.listObjects(bucket, **kwargs)

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
# 提取对象索引行
# ================================
def _extract_rows(body):
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
    return rows


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
    shard_target = max(worker_count * 4, 16)

    logging.info(
        "[OBS_INDEX] start build index prefix=%s workers=%s",
        root_prefix,
        worker_count,
    )

    # ================================
    # 去重前缀
    # ================================
    def mark_prefix(current_prefix):
        normalized = sanitize_key(normalize_obs_key(current_prefix or ""))
        with seen_lock:
            if normalized in seen_prefixes:
                return None
            seen_prefixes.add(normalized)
        return normalized

    # ================================
    # 写入一批索引行
    # ================================
    def save_rows(rows):
        nonlocal total
        if not rows:
            return

        checkpoint.upsert_obs_many(rows)
        with total_lock:
            total += len(rows)
            if total and total % 100000 == 0:
                logging.info("[OBS_INDEX] cached %s objects", total)

    # ================================
    # 仅按当前层级发现子前缀
    # ================================
    def expand_prefix(client, current_prefix):
        child_prefixes = []
        marker = None

        while not inner_stop_event.is_set():
            if stop_event is not None and stop_event.is_set():
                inner_stop_event.set()
                return child_prefixes
            response = _list_objects(
                client,
                bucket,
                current_prefix,
                marker,
                delimiter="/",
                low_level_retries=low_level_retries,
                low_level_retry_sleep=low_level_retry_sleep,
                governor=governor,
            )

            if response.status >= 300:
                raise RuntimeError(f"OBS list error {response.status}")

            body = getattr(response, "body", None)
            if body is None:
                return child_prefixes

            for child_prefix in _extract_common_prefixes(body):
                normalized = mark_prefix(child_prefix)
                if normalized is not None:
                    child_prefixes.append(normalized)

            save_rows(_extract_rows(body))

            if not getattr(body, "is_truncated", False):
                return child_prefixes

            marker = getattr(body, "next_marker", None)

        return child_prefixes

    # ================================
    # 平铺扫描单个分片前缀
    # ================================
    def scan_prefix_flat(client, current_prefix):
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
                delimiter=None,
                low_level_retries=low_level_retries,
                low_level_retry_sleep=low_level_retry_sleep,
                governor=governor,
            )

            if response.status >= 300:
                raise RuntimeError(f"OBS list error {response.status}")

            body = getattr(response, "body", None)
            if body is None:
                return

            save_rows(_extract_rows(body))

            if not getattr(body, "is_truncated", False):
                return

            marker = getattr(body, "next_marker", None)

    # ================================
    # 发现适合并发的分片前缀
    # ================================
    def discover_frontier(client):
        frontier = deque()
        root = mark_prefix(root_prefix)
        frontier.append(root_prefix if root is None else root)
        expanded = 0

        while frontier and len(frontier) < shard_target and not inner_stop_event.is_set():
            if stop_event is not None and stop_event.is_set():
                inner_stop_event.set()
                break

            current_prefix = frontier.popleft()
            child_prefixes = expand_prefix(client, current_prefix)
            expanded += 1

            if child_prefixes:
                frontier.extend(child_prefixes)

        logging.info(
            "[OBS_INDEX] frontier=%s expanded_prefixes=%s shard_target=%s",
            len(frontier),
            expanded,
            shard_target,
        )
        return list(frontier)

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
                scan_prefix_flat(client, current_prefix)
            except Exception as exc:
                inner_stop_event.set()
                with error_lock:
                    if first_error[0] is None:
                        first_error[0] = exc
                logging.exception("[OBS_INDEX][SHARD_ERROR] prefix=%s", current_prefix)
            finally:
                prefix_queue.task_done()

    discovery_client = create_client()
    shard_prefixes = discover_frontier(discovery_client)
    if first_error[0] is not None:
        checkpoint.flush_obs_index()
        raise first_error[0]

    if inner_stop_event.is_set() and (stop_event is None or stop_event.is_set()):
        checkpoint.flush_obs_index()
        logging.warning("[OBS_INDEX] stopped before shard scanning total=%s", total)
        return False

    for current_prefix in shard_prefixes:
        prefix_queue.put(current_prefix)

    threads = []
    actual_workers = max(1, min(worker_count, max(len(shard_prefixes), 1)))
    for _ in range(actual_workers):
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
        for _ in range(actual_workers):
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
