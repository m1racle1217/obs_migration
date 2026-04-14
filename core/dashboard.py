# core/dashboard.py
# -*- coding: utf-8 -*-

import time

from rich.console import Console
from rich.live import Live
from rich.table import Table


class Dashboard:

    def __init__(
        self,
        progress,
        task_queue,
        scheduler,
        scan_workers,
        enabled=True,
        force_terminal=False,
        status_provider=None,
    ):

        self.progress = progress
        self.task_queue = task_queue
        self.scheduler = scheduler
        self.scan_workers = scan_workers
        self.enabled = enabled
        self.console = Console(force_terminal=force_terminal)
        self.status_provider = status_provider
        self.running = False

    def start(self):

        self.running = True

    def stop(self):

        self.running = False

    def build_table(self):

        snapshot = self.progress.snapshot()

        done = snapshot["done_bytes"]
        total = snapshot["total_bytes"]
        files_done = snapshot["files_done"]
        files_skip = snapshot["files_skip"]
        scan_skip = snapshot["scan_skip"]
        scan_files = snapshot["scan_files"]
        scan_errors = snapshot["scan_errors"]
        upload_errors = snapshot["upload_errors"]
        cache_hit = snapshot["cache_hit"]
        cache_total = snapshot["cache_total"]
        active_workers = self.scheduler.get_active_workers()

        status = {}
        if self.status_provider is not None:
            status = self.status_provider()

        index_status = status.get("index", "unknown")
        scan_status = status.get("scan", "unknown")

        if active_workers > 0:
            upload_status = f"running ({active_workers} active)"
        elif self.task_queue.unfinished_tasks > 0:
            upload_status = "queued"
        elif scan_status in {"pending", "running"}:
            upload_status = "waiting for scan"
        else:
            upload_status = "idle"

        hit_rate = 0
        if cache_total > 0:
            hit_rate = cache_hit / cache_total * 100

        scan_elapsed = max(time.time() - snapshot["scan_start"], 0.001)
        scan_speed = scan_files / scan_elapsed

        elapsed = max(time.time() - snapshot["start_time"], 0.001)
        upload_speed = done / elapsed / 1024 / 1024

        table = Table(title="OBS Migration Dashboard")
        table.add_column("Metric")
        table.add_column("Value")

        table.add_row("Files Done", str(files_done))
        table.add_row("Upload Skip", str(files_skip))
        table.add_row("Scan Skip", str(scan_skip))
        table.add_row("Index Status", index_status)
        table.add_row("Scan Status", scan_status)
        table.add_row("Upload Status", upload_status)
        table.add_row("Cache Hit", f"{cache_hit}/{cache_total}")
        table.add_row("Hit Rate", f"{hit_rate:.1f}%")
        table.add_row(
            "Progress",
            f"{done / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB",
        )
        table.add_row("Scan Files", str(scan_files))
        table.add_row("Scan Speed", f"{scan_speed:.0f} files/s")
        table.add_row("Scan Errors", str(scan_errors))
        table.add_row("Upload Errors", str(upload_errors))
        table.add_row("Upload Speed", f"{upload_speed:.1f} MB/s")
        table.add_row("Queue Size", str(self.task_queue.qsize()))
        table.add_row("Upload Workers", str(len(self.scheduler.threads)))
        table.add_row("Scan Workers", str(self.scan_workers))

        return table

    def run_until(self, done_fn, poll_interval=0.2, start_fn=None):

        self.running = True

        if not self.enabled:
            if start_fn is not None:
                start_fn()
            while self.running and not done_fn():
                time.sleep(poll_interval)
            return

        with Live(
            self.build_table(),
            refresh_per_second=max(1, int(1 / poll_interval)),
            console=self.console,
            transient=False,
        ) as live:
            live.update(self.build_table(), refresh=True)

            if start_fn is not None:
                start_fn()

            while self.running:
                live.update(self.build_table(), refresh=True)

                if done_fn():
                    break

                time.sleep(poll_interval)

            live.update(self.build_table(), refresh=True)
