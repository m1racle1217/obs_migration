# core/progress.py
# -*- coding: utf-8 -*-

import threading
import time

from rich.live import Live
from rich.table import Table
from rich.progress import (
    Progress as RichProgress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    DownloadColumn
)


class Progress:

    def __init__(self):

        # 数据统计
        self.total_bytes = 0
        self.done_bytes = 0

        self.files_done = 0
        self.files_skip = 0
        self.scan_files = 0
        self.scan_errors = 0
        self.upload_errors = 0
        self.scan_skip = 0

        self.scan_start = time.time()

        self.start_time = time.time()

        self.lock = threading.Lock()

        self.running = False

        self._thread = None

    # =============================
    # 启动进度系统
    # =============================

    def start(self):

        self.running = True

        self._thread = threading.Thread(
            target=self._loop,
            daemon=True
        )

        self._thread.start()

    # =============================
    # 停止
    # =============================

    def stop(self):

        self.running = False

        if self._thread:
            self._thread.join()

    # =============================
    # 扫描增加总量
    # =============================

    def add_total(self, size):

        with self.lock:
            self.total_bytes += size

    # =============================
    # 完成
    # =============================

    def add_done(self, size):

        with self.lock:

            self.done_bytes += size
            self.files_done += 1

    # =============================
    # 跳过
    # =============================

    def skip(self):

        with self.lock:
            self.files_skip += 1

    def scan_skip_inc(self, n=1):
        with self.lock:
            self.scan_skip += n

    # =============================
    # UI 渲染
    # =============================

    def _loop(self):

        progress = RichProgress(

            TextColumn("[bold blue]OBS Upload"),

            BarColumn(),

            DownloadColumn(),

            TransferSpeedColumn(),

            TimeRemainingColumn(),
        )

        task = progress.add_task(
            "upload",
            total=1
        )

        table = Table()

        table.add_column("Metric")
        table.add_column("Value")

        with Live(progress, refresh_per_second=4):

            while self.running:

                with self.lock:

                    total = self.total_bytes
                    done = self.done_bytes

                    files_done = self.files_done
                    files_skip = self.files_skip

                if total > 0:

                    progress.update(
                        task,
                        total=total,
                        completed=done
                    )

                time.sleep(0.5)