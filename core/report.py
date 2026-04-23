# core/reporter.py
# -*- coding: utf-8 -*-
"""生成迁移结果与未完成任务的 CSV / JSON 报告。"""

import csv
import json
import os
import threading
import time


# ================================
# 输出迁移结果报告
# ================================
class Reporter:
    """跟踪任务生命周期事件，并持续刷新到报告文件。"""

    # ================================
    # 初始化报告器
    # ================================
    def __init__(self, report_dir, source_label):

        os.makedirs(report_dir, exist_ok=True)

        name = os.path.basename(os.path.normpath(source_label)) or "root"
        ts = time.strftime("%Y%m%d_%H%M%S")

        self.file = os.path.join(report_dir, f"{name}_{ts}.csv")
        self.summary_file = os.path.join(report_dir, f"{name}_{ts}_summary.json")

        self.fp = open(self.file, "w", newline="", encoding="utf-8", errors="ignore")
        self.writer = csv.writer(self.fp)
        self.writer.writerow([
            "source_path",
            "target_path",
            "size",
            "status",
            "message",
        ])

        self.lock = threading.Lock()
        self.buffer = []
        self.flush_size = 500
        self.pending_tasks = {}
        self.completed_sources = set()
        self.closed = False

        self.stats = {
            "SUCCESS": 0,
            "UPLOAD_SKIP": 0,
            "SCAN_SKIP": 0,
            "FAILED": 0,
            "MISSING": 0,
            "ERROR": 0,
            "INTERRUPTED": 0,
            "UNFINISHED": 0,
            "UNKNOWN": 0,
            "SUCCESS_TOTAL": 0,
            "TOTAL_SIZE": 0,
            "START_TIME": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        self.running = True
        self.flush_interval = 5
        self._start_auto_flush()

    # ================================
    # 安全转为 UTF-8
    # ================================
    def _safe_utf8(self, value):
        if isinstance(value, bytes):
            return repr(value)
        if isinstance(value, str):
            return value.encode("utf-8", "ignore").decode("utf-8", "ignore")
        return str(value)

    # ================================
    # 归一化状态值
    # ================================
    def _normalize_status(self, status):
        normalized = (status or "UNKNOWN").upper()
        if normalized == "SKIP":
            return "UPLOAD_SKIP"
        if normalized == "SKIP_SCAN":
            return "SCAN_SKIP"
        return normalized

    # ================================
    # 更新统计信息
    # ================================
    def _update_stats(self, status, size):
        if status not in self.stats:
            self.stats[status] = 0

        self.stats[status] += 1

        if status in ("SUCCESS", "UPLOAD_SKIP"):
            self.stats["SUCCESS_TOTAL"] += 1
            self.stats["TOTAL_SIZE"] += int(size or 0)

    # ================================
    # 启动自动刷新线程
    # ================================
    def _start_auto_flush(self):

        # ================================
        # 循环刷新缓冲区
        # ================================
        def loop():
            while self.running:
                time.sleep(self.flush_interval)
                with self.lock:
                    if self.buffer:
                        self._flush()

        self.flush_thread = threading.Thread(target=loop, daemon=True)
        self.flush_thread.start()

    # ================================
    # 跟踪待迁移任务
    # ================================
    def track_task(self, source_path, size=0, target_path="", msg=""):
        source = self._safe_utf8(source_path)
        if not source:
            return

        with self.lock:
            if self.closed or source in self.completed_sources or source in self.pending_tasks:
                return

            self.pending_tasks[source] = {
                "target_path": self._safe_utf8(target_path),
                "size": int(size or 0),
                "message": self._safe_utf8(msg),
            }

    # ================================
    # 写入结果记录
    # ================================
    def write(self, local, obs, size=0, status="UNKNOWN", msg=""):
        source = self._safe_utf8(local)
        target = self._safe_utf8(obs)
        normalized_status = self._normalize_status(status)
        row = [
            source,
            target,
            int(size or 0),
            normalized_status,
            self._safe_utf8(msg),
        ]

        with self.lock:
            if source:
                self.pending_tasks.pop(source, None)
                self.completed_sources.add(source)

            self.buffer.append(row)
            self._update_stats(normalized_status, size)

            if len(self.buffer) >= self.flush_size:
                self._flush()

    # ================================
    # 刷新缓存到磁盘
    # ================================
    def _flush(self):
        try:
            self.writer.writerows(self.buffer)
            self.fp.flush()
        except Exception as exc:
            print(f"[REPORT][FLUSH_ERROR] {exc}")

        self.buffer.clear()

    # ================================
    # 补写未完成任务
    # ================================
    def _flush_pending_locked(self, status, msg):
        normalized_status = self._normalize_status(status)
        default_message = self._safe_utf8(msg)

        for source, data in list(self.pending_tasks.items()):
            row = [
                source,
                "",
                int(data.get("size", 0) or 0),
                normalized_status,
                default_message or data.get("message") or "detected_but_not_migrated",
            ]
            self.buffer.append(row)
            self.completed_sources.add(source)
            self._update_stats(normalized_status, data.get("size", 0))

        self.pending_tasks.clear()

    # ================================
    # 写入汇总文件
    # ================================
    def _write_summary(self):
        self.stats["END_TIME"] = time.strftime("%Y-%m-%d %H:%M:%S")

        output = dict(self.stats)
        counted_statuses = [
            key for key, value in output.items()
            if isinstance(value, int) and key not in {"SUCCESS_TOTAL", "TOTAL_SIZE"}
        ]
        output["TOTAL_FILES"] = sum(output[key] for key in counted_statuses)
        output["EFFECTIVE_FILES"] = (
            output.get("SUCCESS", 0) +
            output.get("UPLOAD_SKIP", 0)
        )

        try:
            with open(self.summary_file, "w", encoding="utf-8") as handle:
                json.dump(output, handle, indent=4, ensure_ascii=False)
        except Exception as exc:
            print(f"[REPORT] write summary failed: {exc}")

    # ================================
    # 关闭报告器
    # ================================
    def close(self, pending_status=None, pending_message=""):
        self.running = False

        if hasattr(self, "flush_thread"):
            self.flush_thread.join(timeout=2)

        with self.lock:
            unresolved_status = pending_status
            if unresolved_status is None and self.pending_tasks:
                unresolved_status = "UNFINISHED"

            if unresolved_status is not None and self.pending_tasks:
                self._flush_pending_locked(
                    unresolved_status,
                    pending_message or "detected_but_not_migrated",
                )

            if self.buffer:
                self._flush()

            self.closed = True

        try:
            self.fp.close()
        except Exception:
            pass

        self._write_summary()

        print(f"\n[REPORT] saved: {self.file}")
        print(f"[SUMMARY] saved: {self.summary_file}")
