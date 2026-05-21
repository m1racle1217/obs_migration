# core/scanner.py
# -*- coding: utf-8 -*-
"""扫描本地文件系统并将待迁移文件送入任务队列。"""

import logging
import os
import queue
import threading
import time

from .utils import clean_path_to_utf8, normalize_relative_path, safe_log

SCAN_BATCH = 1000

IGNORE_SUFFIX = (
    ".upload_record",
    ".tmp",
    ".part",
)

IGNORE_FILES = (
    ".DS_Store",
    "Thumbs.db",
)


# ================================
# 扫描本地目录
# ================================
def _scan_local_entries(
    entries,
    task_queue,
    progress,
    checkpoint,
    reporter=None,
    scan_workers=4,
    scan_done_event=None,
    scan_controller=None,
    controls=None,
    excluded_roots=None,
):
    stop_token = object()

    logging.info(
        "[SCAN] scanning local entries=%s (workers=%s)",
        len(entries),
        scan_workers,
    )

    start_time = time.time()
    scan_queue = queue.Queue()

    total_scanned = 0
    scanned_lock = threading.Lock()

    def stop_requested():
        return controls is not None and controls.stop_requested()

    def wait_if_paused():
        if controls is not None:
            controls.wait_if_paused(poll_interval=0.05)

    def normalize_excluded_root(path):
        try:
            return os.path.normcase(os.path.abspath(os.fsdecode(path)))
        except Exception:
            return ""

    excluded_root_paths = [
        path for path in (normalize_excluded_root(path) for path in (excluded_roots or [])) if path
    ]

    def is_excluded(path_bytes):
        if not excluded_root_paths:
            return False

        try:
            current_path = os.path.normcase(os.path.abspath(os.fsdecode(path_bytes)))
        except Exception:
            return False

        for root_path in excluded_root_paths:
            if current_path == root_path:
                return True
            try:
                if os.path.commonpath([current_path, root_path]) == root_path:
                    return True
            except ValueError:
                continue
        return False

    def defer_claimed_item(item):
        scan_queue.put(item)
        scan_queue.task_done()

    def enqueue_output_task(task):
        if controls is None:
            task_queue.put(task)
            return True

        while not stop_requested():
            try:
                task_queue.put(task, timeout=0.05)
                return True
            except queue.Full:
                continue

        return False

    # ================================
    # 计算相对路径
    # ================================
    def build_relative_path(local_path_bytes, base_dir_bytes):
        try:
            relative_bytes = os.path.relpath(local_path_bytes, base_dir_bytes)
        except Exception:
            relative_bytes = local_path_bytes
        return normalize_relative_path(relative_bytes)

    # ================================
    # 记录扫描跳过项
    # ================================
    def report_skip(path_bytes, reason):
        progress.scan_skip_inc()
        if reporter is None:
            return

        try:
            reporter.write(
                local=path_bytes,
                obs="",
                size=0,
                status="SKIP_SCAN",
                msg=reason,
            )
        except Exception:
            pass

    # ================================
    # 处理单个文件
    # ================================
    def handle_file(local_path_bytes, base_dir_bytes):
        nonlocal total_scanned

        wait_if_paused()
        if stop_requested():
            return

        if is_excluded(local_path_bytes):
            report_skip(local_path_bytes, "runtime_output")
            return

        clean_name = clean_path_to_utf8(os.path.basename(local_path_bytes))

        if clean_name in IGNORE_FILES:
            report_skip(local_path_bytes, f"ignore_file({clean_name})")
            return

        if clean_name.startswith("."):
            report_skip(local_path_bytes, "hidden_file")
            return

        for suffix in IGNORE_SUFFIX:
            if clean_name.endswith(suffix):
                report_skip(local_path_bytes, f"ignore_suffix({suffix})")
                return

        stat_result = os.stat(local_path_bytes)
        size = stat_result.st_size
        source_path = clean_path_to_utf8(local_path_bytes)
        relative_path = build_relative_path(local_path_bytes, base_dir_bytes)

        wait_if_paused()
        if stop_requested():
            return

        task = {
            "source_type": "local",
            "local": local_path_bytes,
            "source_path": source_path,
            "relative_path": relative_path,
            "size": size,
            "mtime": stat_result.st_mtime,
        }
        if not enqueue_output_task(task):
            return

        if reporter is not None and hasattr(reporter, "track_task"):
            reporter.track_task(source_path, size=size)

        progress.record_scan_file(size)

        with scanned_lock:
            total_scanned += 1
            if total_scanned % SCAN_BATCH == 0:
                logging.info("[SCAN] scanned %s local files", total_scanned)

    # ================================
    # 执行目录扫描工作线程
    # ================================
    def worker():
        while True:
            wait_if_paused()

            if scan_controller is not None and not scan_controller.acquire_slot():
                return

            wait_if_paused()
            current_item = scan_queue.get()

            if current_item is stop_token:
                if scan_controller is not None:
                    scan_controller.release_slot()
                scan_queue.task_done()
                return

            if stop_requested():
                scan_queue.task_done()
                if scan_controller is not None:
                    scan_controller.release_slot()
                continue

            if controls is not None and controls.pause_requested():
                defer_claimed_item(current_item)
                if scan_controller is not None:
                    scan_controller.release_slot()
                continue

            item_type, current_path_bytes, base_dir_bytes = current_item
            progress.scan_worker_started()

            try:
                wait_if_paused()
                if stop_requested():
                    continue

                if is_excluded(current_path_bytes):
                    report_skip(current_path_bytes, "runtime_output")
                    continue

                if item_type == "file":
                    handle_file(current_path_bytes, base_dir_bytes)
                else:
                    with os.scandir(current_path_bytes) as dir_iterator:
                        for entry in dir_iterator:
                            wait_if_paused()
                            if stop_requested():
                                break

                            try:
                                if is_excluded(entry.path):
                                    report_skip(entry.path, "runtime_output")
                                    continue

                                if entry.is_dir(follow_symlinks=False):
                                    wait_if_paused()
                                    if stop_requested():
                                        break
                                    scan_queue.put(("dir", entry.path, base_dir_bytes))
                                    continue

                                if not entry.is_file():
                                    continue

                                handle_file(entry.path, base_dir_bytes)
                            except Exception as inner_error:
                                progress.scan_error_inc()
                                report_skip(entry.path, f"scan_error({str(inner_error)[:30]})")
                                logging.error(
                                    "[SCAN][FILE_ERROR] %s [%s]",
                                    safe_log(entry.path),
                                    inner_error,
                                )
            except Exception as outer_error:
                logging.error(
                    "[SCAN][ENTRY_ERROR] %s [%s]",
                    safe_log(current_path_bytes),
                    outer_error,
                )
            finally:
                progress.scan_worker_finished()
                scan_queue.task_done()
                if scan_controller is not None:
                    scan_controller.release_slot()

    for entry in entries:
        path_value = entry.get("path")
        base_dir_value = entry.get("base_dir") or path_value
        item_type = entry.get("type") or "dir"

        scan_queue.put(
            (
                item_type,
                os.fsencode(path_value),
                os.fsencode(base_dir_value),
            )
        )

    threads = []
    for _ in range(scan_workers):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        threads.append(thread)

    scan_queue.join()

    if scan_done_event is not None:
        scan_done_event.set()

    if scan_controller is not None:
        scan_controller.stop()

    for _ in range(scan_workers):
        scan_queue.put(stop_token)

    for thread in threads:
        thread.join()

    elapsed_time = max(time.time() - start_time, 0.001)
    logging.info(
        "[SCAN_DONE] total=%s cost=%.1fs speed=%.1f file/s",
        total_scanned,
        elapsed_time,
        total_scanned / elapsed_time,
    )


# ================================
# 扫描本地目录
# ================================
def scan_directory(
    root_dir,
    task_queue,
    progress,
    checkpoint,
    reporter=None,
    scan_workers=4,
    scan_done_event=None,
    scan_controller=None,
    base_dir=None,
    controls=None,
    excluded_roots=None,
):
    root_dir = root_dir or ""
    effective_base_dir = base_dir or root_dir
    logging.info("[SCAN] scanning local path=%s (workers=%s)", root_dir, scan_workers)
    return _scan_local_entries(
        [{"type": "dir", "path": root_dir, "base_dir": effective_base_dir}],
        task_queue,
        progress,
        checkpoint,
        reporter=reporter,
        scan_workers=scan_workers,
        scan_done_event=scan_done_event,
        scan_controller=scan_controller,
        controls=controls,
        excluded_roots=excluded_roots,
    )


# ================================
# 扫描多组本地源
# ================================
def scan_local_sources(
    entries,
    task_queue,
    progress,
    checkpoint,
    reporter=None,
    scan_workers=4,
    scan_done_event=None,
    scan_controller=None,
    controls=None,
    excluded_roots=None,
):
    return _scan_local_entries(
        entries,
        task_queue,
        progress,
        checkpoint,
        reporter=reporter,
        scan_workers=scan_workers,
        scan_done_event=scan_done_event,
        scan_controller=scan_controller,
        controls=controls,
        excluded_roots=excluded_roots,
    )
