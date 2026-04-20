# core/progress.py
# -*- coding: utf-8 -*-

import threading
import time


class Progress:

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

    def start(self):

        self.running = True

    def stop(self):

        self.running = False

    def add_total(self, size):

        with self.lock:
            self.total_bytes += size

    def add_done(self, size):

        with self.lock:
            self.done_bytes += size
            self.files_done += 1

    def skip(self):

        with self.lock:
            self.files_skip += 1

    def scan_skip_inc(self, n=1):

        with self.lock:
            self.scan_skip += n

    def scan_error_inc(self, n=1):

        with self.lock:
            self.scan_errors += n

    def scan_worker_started(self):

        with self.lock:
            self.scan_active_workers += 1

    def scan_worker_finished(self):

        with self.lock:
            if self.scan_active_workers > 0:
                self.scan_active_workers -= 1

    def upload_error_inc(self, n=1):

        with self.lock:
            self.upload_errors += n

    def record_scan_file(self, size):

        with self.lock:
            self.total_bytes += size
            self.scan_files += 1

    def cache_hit_inc(self):

        with self.lock:
            self.cache_hit += 1
            self.cache_total += 1

    def cache_miss_inc(self):

        with self.lock:
            self.cache_total += 1

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
