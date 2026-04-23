# core/scanner.py
# -*- coding: utf-8 -*-

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


def scan_directory(
    root_dir,
    task_queue,
    progress,
    checkpoint,
    reporter=None,
    scan_workers=4,
    scan_done_event=None,
):
    root_dir_bytes = os.fsencode(root_dir)
    stop_token = object()

    logging.info("[SCAN] scanning local path=%s (workers=%s)", root_dir, scan_workers)

    start_time = time.time()
    dir_queue = queue.Queue()
    dir_queue.put(root_dir_bytes)

    total_scanned = 0
    scanned_lock = threading.Lock()

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

    def worker():
        nonlocal total_scanned

        while True:
            current_dir_bytes = dir_queue.get()

            if current_dir_bytes is stop_token:
                dir_queue.task_done()
                return

            progress.scan_worker_started()

            try:
                with os.scandir(current_dir_bytes) as dir_iterator:
                    for entry in dir_iterator:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                dir_queue.put(entry.path)
                                continue

                            if not entry.is_file():
                                continue

                            clean_name = clean_path_to_utf8(entry.name)

                            if clean_name in IGNORE_FILES:
                                report_skip(entry.path, f"ignore_file({clean_name})")
                                continue

                            if clean_name.startswith("."):
                                report_skip(entry.path, "hidden_file")
                                continue

                            hit_suffix = False
                            for suffix in IGNORE_SUFFIX:
                                if clean_name.endswith(suffix):
                                    report_skip(entry.path, f"ignore_suffix({suffix})")
                                    hit_suffix = True
                                    break
                            if hit_suffix:
                                continue

                            local_path_bytes = entry.path
                            relative_bytes = local_path_bytes[len(root_dir_bytes):].lstrip(b"/\\")
                            relative_path = normalize_relative_path(relative_bytes)

                            st = entry.stat()
                            size = st.st_size

                            task_queue.put(
                                {
                                    "source_type": "local",
                                    "local": local_path_bytes,
                                    "source_path": clean_path_to_utf8(local_path_bytes),
                                    "relative_path": relative_path,
                                    "size": size,
                                    "mtime": st.st_mtime,
                                }
                            )

                            progress.record_scan_file(size)

                            with scanned_lock:
                                total_scanned += 1
                                if total_scanned % SCAN_BATCH == 0:
                                    logging.info("[SCAN] scanned %s local files", total_scanned)
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
                    "[SCAN][DIR_ERROR] %s [%s]",
                    safe_log(current_dir_bytes),
                    outer_error,
                )
            finally:
                progress.scan_worker_finished()
                dir_queue.task_done()

    threads = []
    for _ in range(scan_workers):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        threads.append(thread)

    dir_queue.join()

    if scan_done_event is not None:
        scan_done_event.set()

    for _ in range(scan_workers):
        dir_queue.put(stop_token)

    for thread in threads:
        thread.join()

    elapsed_time = max(time.time() - start_time, 0.001)
    logging.info(
        "[SCAN_DONE] total=%s cost=%.1fs speed=%.1f file/s",
        total_scanned,
        elapsed_time,
        total_scanned / elapsed_time,
    )
