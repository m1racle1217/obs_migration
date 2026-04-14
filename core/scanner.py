# core/scanner.py
# -*- coding: utf-8 -*-

"""
Directory Scanner Module（生产级修正版 + 可审计增强）

保证：
✔ 原扫描逻辑完全不变
✔ bytes 路径不破坏
✔ 编码安全
✔ 传递 size 给 uploader（用于 HEAD 判断）
✔ 更干净职责：只负责“发现任务”
✔ ✅ 新增：扫描阶段忽略原因上报（可选）
"""

import os
import logging
import time
import threading
import queue

from .utils import (
    normalize_obs_key,
    sanitize_key,
    safe_log,
    clean_path_to_utf8,
    normalize_relative_path
)

SCAN_BATCH = 1000

IGNORE_SUFFIX = (
    ".upload_record",
    ".tmp",
    ".part"
)

IGNORE_FILES = (
    ".DS_Store",
    "Thumbs.db"
)


def scan_directory(
        root_dir,
        obs_prefix,
        task_queue,
        progress,
        checkpoint,
        reporter=None,
        scan_workers = 4,
        scan_done_event=None,
):

    root_dir_bytes = os.fsencode(root_dir)
    stop_token = object()

    logging.info(
        f"[SCAN] 扫描目录: {root_dir} "
        f"(workers={scan_workers})"
    )

    start_time = time.time()

    dir_queue = queue.Queue()
    dir_queue.put(root_dir_bytes)

    total_scanned = 0
    scanned_lock = threading.Lock()

    # ==========================================================
    # ✅ 查询 OBS 索引,统一 skip 上报（仅扫描阶段）
    # ==========================================================

    def report_skip(path_bytes, reason):
        progress.scan_skip_inc()
        if not reporter:
            return
        try:
            reporter.write(
                local=path_bytes,
                obs="",
                size=0,
                status="SKIP_SCAN",
                msg=reason
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

                            # -------------------------
                            # 目录处理
                            # -------------------------
                            if entry.is_dir(follow_symlinks=False):
                                dir_queue.put(entry.path)
                                continue

                            # -------------------------
                            # 文件判断
                            # -------------------------
                            if not entry.is_file():
                                continue

                            clean_name = clean_path_to_utf8(entry.name)

                            # -------------------------
                            # ✅ 忽略：文件名
                            # -------------------------
                            if clean_name in IGNORE_FILES:
                                report_skip(entry.path, f"ignore_file({clean_name})")
                                continue

                            # -------------------------
                            # ✅ 忽略：隐藏文件
                            # -------------------------
                            if clean_name.startswith("."):
                                report_skip(entry.path, "hidden_file")
                                continue

                            # -------------------------
                            # ✅ 忽略：后缀
                            # -------------------------
                            hit_suffix = False
                            for suf in IGNORE_SUFFIX:
                                if clean_name.endswith(suf):
                                    report_skip(entry.path, f"ignore_suffix({suf})")
                                    hit_suffix = True
                                    break
                            if hit_suffix:
                                continue

                            # -------------------------
                            # 路径处理
                            # -------------------------
                            local_path_bytes = entry.path

                            relative_bytes = local_path_bytes[len(root_dir_bytes):]
                            relative_bytes = relative_bytes.lstrip(b"/\\")

                            relative_clean_str = normalize_relative_path(relative_bytes)

                            cleaned_prefix = obs_prefix.strip("/")

                            obs_key_parts = [cleaned_prefix, relative_clean_str]

                            raw_obs_key = "/".join(filter(None, obs_key_parts))

                            # 防止出现 \（只允许 Windows 转换后出现 /）
                            if "\\" in raw_obs_key:
                                logging.warning(f"[PATH_WARN] backslash in key: {raw_obs_key}")

                            normalized_key = normalize_obs_key(raw_obs_key)
                            final_obs_key = sanitize_key(normalized_key)

                            # -------------------------
                            # 获取文件信息（新增）
                            # -------------------------
                            st = entry.stat()
                            size = st.st_size

                            # -------------------------
                            # 创建任务（增强：带 size）
                            # -------------------------
                            upload_task = {
                                "local": local_path_bytes,
                                "obs": final_obs_key,
                                "size": size
                            }

                            task_queue.put(upload_task)

                            # -------------------------
                            # 统计
                            # -------------------------
                            progress.record_scan_file(size)

                            with scanned_lock:
                                total_scanned += 1

                                if total_scanned % SCAN_BATCH == 0:
                                    logging.info(
                                        f"[SCAN] 已扫描 {total_scanned} 文件"
                                    )

                        except Exception as inner_error:

                            progress.scan_error_inc()

                            report_skip(entry.path, f"scan_error({str(inner_error)[:30]})")

                            logging.error(
                                f"[SCAN][FILE_ERROR] "
                                f"{safe_log(entry.path)} "
                                f"[{inner_error}]"
                            )

            except Exception as outer_error:

                logging.error(
                    f"[SCAN][DIR_ERROR] "
                    f"{safe_log(current_dir_bytes)} "
                    f"[{outer_error}]"
                )

            finally:
                progress.scan_worker_finished()
                dir_queue.task_done()

    # 启动线程
    threads = []

    for _ in range(scan_workers):

        t = threading.Thread(target=worker, daemon=True)
        t.start()

        threads.append(t)

    dir_queue.join()

    if scan_done_event is not None:
        scan_done_event.set()

    for _ in range(scan_workers):
        dir_queue.put(stop_token)

    for t in threads:
        t.join()

    elapsed_time = time.time() - start_time

    logging.info(
        f"[SCAN_DONE] total={total_scanned} "
        f"cost={elapsed_time:.1f}s "
        f"speed={total_scanned/elapsed_time:.1f} file/s"
    )
