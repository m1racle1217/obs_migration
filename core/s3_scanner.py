
# -*- coding: utf-8 -*-

import logging
import queue
import threading
import time

from .utils import build_object_uri, normalize_obs_key, sanitize_key, to_unix_timestamp


def _normalize_prefix(prefix):
    return sanitize_key(normalize_obs_key(prefix or "")).strip("/")


def _build_relative_key(source_key, source_prefix):
    source_key = sanitize_key(normalize_obs_key(source_key or "")).strip("/")
    source_prefix = _normalize_prefix(source_prefix)

    relative_key = source_key
    if source_prefix:
        source_prefix_with_sep = source_prefix + "/"
        if source_key == source_prefix:
            relative_key = source_key.rsplit("/", 1)[-1]
        elif source_key.startswith(source_prefix_with_sep):
            relative_key = source_key[len(source_prefix_with_sep):]

    return relative_key


def _extract_common_prefixes(body):
    for attr in ("commonPrefixs", "commonPrefixes", "commonPrefixList"):
        items = getattr(body, attr, None)
        if not items:
            continue

        for item in items:
            prefix = getattr(item, "prefix", None)
            if prefix is None and isinstance(item, str):
                prefix = item
            if prefix:
                yield sanitize_key(normalize_obs_key(prefix or ""))


def _list_objects(source_client, source_bucket, current_prefix, marker, delimiter=None):
    kwargs = {
        "prefix": current_prefix,
        "marker": marker,
        "max_keys": 1000,
    }
    if delimiter:
        try:
            return source_client.listObjects(
                source_bucket,
                delimiter=delimiter,
                **kwargs,
            )
        except TypeError:
            pass

    return source_client.listObjects(source_bucket, **kwargs)


def scan_s3_objects(
    source_client,
    source_bucket,
    source_prefix,
    task_queue,
    progress,
    reporter=None,
    scan_workers=1,
    scan_done_event=None,
    source_scheme="s3",
):
    total_scanned = 0
    source_prefix = _normalize_prefix(source_prefix)
    start_time = time.time()
    prefix_queue = queue.Queue()
    stop_token = object()
    total_lock = threading.Lock()
    seen_lock = threading.Lock()
    error_lock = threading.Lock()
    stop_event = threading.Event()
    seen_prefixes = set()
    first_error = [None]

    logging.info(
        "[S3_SCAN] scanning source bucket=%s prefix=%s workers=%s",
        source_bucket,
        source_prefix,
        scan_workers,
    )

    def enqueue_prefix(prefix):
        normalized = sanitize_key(normalize_obs_key(prefix or ""))
        with seen_lock:
            if normalized in seen_prefixes:
                return
            seen_prefixes.add(normalized)
        prefix_queue.put(normalized)

    def scan_prefix(current_prefix):
        nonlocal total_scanned

        marker = None
        while not stop_event.is_set():
            resp = _list_objects(
                source_client,
                source_bucket,
                current_prefix,
                marker,
                delimiter="/",
            )

            if resp.status >= 300:
                raise RuntimeError(f"source list error {resp.status}")

            body = getattr(resp, "body", None)
            if body is None:
                break

            for child_prefix in _extract_common_prefixes(body):
                enqueue_prefix(child_prefix)

            for obj in getattr(body, "contents", []) or []:
                normalized_source_key = sanitize_key(
                    normalize_obs_key(getattr(obj, "key", "") or "")
                )
                source_key = normalized_source_key.strip("/")
                size = int(getattr(obj, "size", 0) or 0)

                if not source_key:
                    continue

                source_ref = build_object_uri(source_bucket, source_key, scheme="s3")
                source_display = build_object_uri(
                    source_bucket,
                    source_key,
                    scheme=source_scheme,
                )

                if normalized_source_key.endswith("/") and size == 0:
                    progress.scan_skip_inc()
                    if reporter is not None:
                        reporter.write(
                            local=source_display,
                            obs="",
                            size=0,
                            status="SKIP_SCAN",
                            msg="directory_marker",
                        )
                    continue

                task_queue.put(
                    {
                        "source_type": "s3",
                        "source_bucket": source_bucket,
                        "source_key": source_key,
                        "source_path": source_ref,
                        "source_display": source_display,
                        "relative_path": _build_relative_key(source_key, source_prefix),
                        "size": size,
                        "mtime": to_unix_timestamp(getattr(obj, "lastModified", None)),
                        "etag": getattr(obj, "etag", None),
                    }
                )

                progress.record_scan_file(size)
                with total_lock:
                    total_scanned += 1
                    if total_scanned % 1000 == 0:
                        logging.info("[S3_SCAN] scanned %s objects", total_scanned)

            if not getattr(body, "is_truncated", False):
                break

            marker = getattr(body, "next_marker", None)

    def worker():
        progress.scan_worker_started()
        try:
            while True:
                current_prefix = prefix_queue.get()
                try:
                    if current_prefix is stop_token:
                        return

                    if stop_event.is_set():
                        continue

                    scan_prefix(current_prefix)
                except Exception as exc:
                    progress.scan_error_inc()
                    stop_event.set()
                    with error_lock:
                        if first_error[0] is None:
                            first_error[0] = exc
                    logging.exception("[S3_SCAN][PREFIX_ERROR] prefix=%s", current_prefix)
                finally:
                    prefix_queue.task_done()
        finally:
            progress.scan_worker_finished()

    worker_count = max(1, int(scan_workers or 1))
    enqueue_prefix(source_prefix)
    threads = []
    for _ in range(worker_count):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        threads.append(thread)

    try:
        prefix_queue.join()
    finally:
        for _ in range(worker_count):
            prefix_queue.put(stop_token)
        for thread in threads:
            thread.join()
        if scan_done_event is not None:
            scan_done_event.set()

    if first_error[0] is not None:
        raise first_error[0]

    elapsed = max(time.time() - start_time, 0.001)
    logging.info(
        "[S3_SCAN_DONE] total=%s cost=%.1fs speed=%.1f obj/s",
        total_scanned,
        elapsed,
        total_scanned / elapsed,
    )
