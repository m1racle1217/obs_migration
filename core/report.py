# core/reporter.py
# -*- coding: utf-8 -*-

import os
import csv
import threading
import time
import json


class Reporter:

    def __init__(self, report_dir, local_dir):

        os.makedirs(report_dir, exist_ok=True)

        name = os.path.basename(os.path.normpath(local_dir)) or "root"
        ts = time.strftime("%Y%m%d_%H%M%S")

        self.file = os.path.join(report_dir, f"{name}_{ts}.csv")
        self.summary_file = os.path.join(report_dir, f"{name}_{ts}_summary.json")

        self.fp = open(self.file, "w", newline="", encoding="utf-8", errors="ignore")
        self.writer = csv.writer(self.fp)

        self.writer.writerow([
            "local_path",
            "obs_key",
            "size",
            "status",
            "message"
        ])

        self.lock = threading.Lock()

        # =============================
        # 缓冲
        # =============================
        self.buffer = []
        self.flush_size = 500

        # =============================
        # 统计信息（升级版）
        # =============================
        self.stats = {
            "SUCCESS": 0,
            "UPLOAD_SKIP": 0,   # ✅ 原 SKIP
            "SCAN_SKIP": 0,     # ✅ 新增
            "FAILED": 0,
            "UNKNOWN": 0,

            "SUCCESS_TOTAL": 0,
            "TOTAL_SIZE": 0,

            "START_TIME": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # =============================
        # 生命周期控制
        # =============================
        self.running = True
        self.flush_interval = 5

        self._start_auto_flush()

    # ==========================================================
    # 自动flush
    # ==========================================================
    def _start_auto_flush(self):

        def loop():
            while self.running:
                time.sleep(self.flush_interval)
                with self.lock:
                    if self.buffer:
                        self._flush()

        self.flush_thread = threading.Thread(
            target=loop,
            daemon=True
        )
        self.flush_thread.start()

    # ==========================================================
    # 写入
    # ==========================================================
    def write(self, local, obs, size=0, status="UNKNOWN", msg=""):

        def _safe_utf8(s):
            if isinstance(s, bytes):
                return repr(s)
            elif isinstance(s, str):
                return s.encode("utf-8", "ignore").decode("utf-8", "ignore")
            return str(s)

        status = (status or "UNKNOWN").upper()

        # =============================
        # 状态映射（关键）
        # =============================
        if status == "SKIP":
            status = "UPLOAD_SKIP"
        elif status == "SKIP_SCAN":
            status = "SCAN_SKIP"

        row = [
            _safe_utf8(local),
            _safe_utf8(obs),
            size,
            status,
            _safe_utf8(msg)
        ]

        with self.lock:

            self.buffer.append(row)

            # =============================
            # 统计
            # =============================
            if status not in self.stats:
                self.stats[status] = 0

            self.stats[status] += 1

            # ✅ 成功口径（只算上传相关）
            if status in ("SUCCESS", "UPLOAD_SKIP"):
                self.stats["SUCCESS_TOTAL"] += 1
                self.stats["TOTAL_SIZE"] += size

            if len(self.buffer) >= self.flush_size:
                self._flush()

    # ==========================================================
    # flush
    # ==========================================================
    def _flush(self):

        try:
            self.writer.writerows(self.buffer)
            self.fp.flush()
        except Exception as e:
            print(f"[REPORT][FLUSH_ERROR] {e}")

        self.buffer.clear()

    # ==========================================================
    # 输出 summary（增强版）
    # ==========================================================
    def _write_summary(self):

        self.stats["END_TIME"] = time.strftime("%Y-%m-%d %H:%M:%S")

        # =============================
        # 派生指标（非常有用）
        # =============================
        output = dict(self.stats)

        output["TOTAL_FILES"] = (
            output.get("SUCCESS", 0) +
            output.get("UPLOAD_SKIP", 0) +
            output.get("FAILED", 0) +
            output.get("SCAN_SKIP", 0)
        )

        output["EFFECTIVE_FILES"] = (
            output.get("SUCCESS", 0) +
            output.get("UPLOAD_SKIP", 0)
        )

        try:
            with open(self.summary_file, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"[REPORT] write summary failed: {e}")

    # ==========================================================
    # 关闭
    # ==========================================================
    def close(self):

        self.running = False

        if hasattr(self, "flush_thread"):
            self.flush_thread.join(timeout=2)

        with self.lock:
            if self.buffer:
                self._flush()

        try:
            self.fp.close()
        except Exception:
            pass

        self._write_summary()

        print(f"\n[REPORT] saved: {self.file}")
        print(f"[SUMMARY] saved: {self.summary_file}")