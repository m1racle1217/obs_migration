# core/dashboard.py
# -*- coding: utf-8 -*-

import time
import threading

from rich.live import Live
from rich.table import Table


class Dashboard:

    def __init__(self, progress, task_queue, scheduler,scan_workers):

        self.progress = progress
        self.task_queue = task_queue
        self.scheduler = scheduler
        self.scan_workers = scan_workers

        self.running = False
        self.thread = None

    # =============================

    def start(self):

        self.running = True

        self.thread = threading.Thread(
            target=self._loop,
            daemon=True
        )

        self.thread.start()

    # =============================

    def stop(self):

        self.running = False

        if self.thread:
            self.thread.join()

    # =============================

    def build_table(self):

        p = self.progress

        table = Table(title="OBS Migration Dashboard")

        table.add_column("Metric")
        table.add_column("Value")

        with p.lock:
            done = p.done_bytes
            total = p.total_bytes

            files_done = p.files_done
            files_skip = p.files_skip
            scan_skip = p.scan_skip

            cache_hit = p.cache_hit
            cache_total = p.cache_total

            hit_rate = 0
            if cache_total > 0:
                hit_rate = cache_hit / cache_total * 100

            scan_elapsed = max(time.time() - p.scan_start, 0.001)
            scan_speed = p.scan_files / scan_elapsed


        elapsed = time.time() - p.start_time

        speed = 0

        if elapsed > 0:
            speed = done / elapsed / 1024 / 1024

        table.add_row("Files Done", str(files_done))
        table.add_row("Upload Skip", str(files_skip))
        table.add_row("Scan Skip", str(scan_skip))

        table.add_row("Cache Hit", f"{cache_hit}/{cache_total}")
        table.add_row("Hit Rate", f"{hit_rate:.1f}%")
        table.add_row(
            "Progress",
            f"{done/1024/1024:.1f}MB / {total/1024/1024:.1f}MB"
        )
        table.add_row("Scan Files", str(p.scan_files))

        table.add_row(
            "Scan Speed",
            f"{scan_speed:.0f} files/s"
        )

        table.add_row("Scan Errors", str(p.scan_errors))
        table.add_row("Upload Errors", str(p.upload_errors))

        table.add_row(
            "Upload Speed",
            f"{speed:.1f} MB/s"
        )

        table.add_row(
            "Queue Size",
            str(self.task_queue.qsize())
        )

        table.add_row(
            "Upload Workers",
            str(len(self.scheduler.threads))
        )
        table.add_row("Scan Workers", str(self.scan_workers))

        return table

    # =============================

    def _loop(self):

        with Live(self.build_table(), refresh_per_second=2) as live:

            while self.running:

                live.update(self.build_table())

                time.sleep(1)