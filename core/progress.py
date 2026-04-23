# core/progress.py
# -*- coding: utf-8 -*-
"""维护线程安全的扫描、上传与校验进度指标。"""

import threading
import time


# ================================
# 汇总运行期进度指标
# ================================
class Progress:
    """集中维护扫描、上传、缓存命中等运行期指标。"""

    # ================================
    # 初始化进度状态
    # ================================
    def __init__(self):

        self.total_bytes = 0
        self.done_bytes = 0

        self.files_done = 0
        self.files_skip = 0
        self.scan_files = 0
        self.scan_errors = 0
        self.upload_errors = 0
        self.scan_skip = 0
        self.scan_active_workers = 0

        self.scan_start = time.time()
        self.start_time = time.time()

        self.cache_hit = 0
        self.cache_total = 0

        self.lock = threading.Lock()
        self.running = False

    # ================================
    # 标记开始
    # ================================
    def start(self):

        self.running = True

    # ================================
    # 标记停止
    # ================================
    def stop(self):

        self.running = False

    # ================================
    # 增加总量
    # ================================
    def add_total(self, size):

        with self.lock:
            self.total_bytes += size

    # ================================
    # 增加完成量
    # ================================
    def add_done(self, size):

        with self.lock:
            self.done_bytes += size
            self.files_done += 1

    # ================================
    # 记录跳过文件
    # ================================
    def skip(self):

        with self.lock:
            self.files_skip += 1

    # ================================
    # 增加扫描跳过数
    # ================================
    def scan_skip_inc(self, n=1):

        with self.lock:
            self.scan_skip += n

    # ================================
    # 增加扫描错误数
    # ================================
    def scan_error_inc(self, n=1):

        with self.lock:
            self.scan_errors += n

    # ================================
    # 标记扫描线程开始
    # ================================
    def scan_worker_started(self):

        with self.lock:
            self.scan_active_workers += 1

    # ================================
    # 标记扫描线程结束
    # ================================
    def scan_worker_finished(self):

        with self.lock:
            if self.scan_active_workers > 0:
                self.scan_active_workers -= 1

    # ================================
    # 增加上传错误数
    # ================================
    def upload_error_inc(self, n=1):

        with self.lock:
            self.upload_errors += n

    # ================================
    # 记录扫描到的文件
    # ================================
    def record_scan_file(self, size):

        with self.lock:
            self.total_bytes += size
            self.scan_files += 1

    # ================================
    # 记录缓存命中
    # ================================
    def cache_hit_inc(self):

        with self.lock:
            self.cache_hit += 1
            self.cache_total += 1

    # ================================
    # 记录缓存未命中
    # ================================
    def cache_miss_inc(self):

        with self.lock:
            self.cache_total += 1

    # ================================
    # 获取进度快照
    # ================================
    def snapshot(self):

        with self.lock:
            return {
                "total_bytes": self.total_bytes,
                "done_bytes": self.done_bytes,
                "files_done": self.files_done,
                "files_skip": self.files_skip,
                "scan_files": self.scan_files,
                "scan_errors": self.scan_errors,
                "upload_errors": self.upload_errors,
                "scan_skip": self.scan_skip,
                "scan_active_workers": self.scan_active_workers,
                "scan_start": self.scan_start,
                "start_time": self.start_time,
                "cache_hit": self.cache_hit,
                "cache_total": self.cache_total,
            }
