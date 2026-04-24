# -*- coding: utf-8 -*-
"""使用 Rich 渲染迁移任务的实时仪表盘。"""

import sys
import time

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.measure import Measurement
from rich.panel import Panel
from rich.progress import BarColumn, Progress as RichProgress, TextColumn
from rich.table import Table
from rich.text import Text


# ================================
# 实时仪表盘
# ================================
class Dashboard:
    """展示扫描、检查、传输与整体进度。"""

    # ================================
    # 初始化仪表盘
    # ================================
    def __init__(
        self,
        progress,
        task_queue,
        scheduler,
        scan_workers,
        checker_queue=None,
        checker_scheduler=None,
        enabled=True,
        force_terminal=False,
        status_provider=None,
        scan_controller=None,
    ):
        self.progress = progress
        self.task_queue = task_queue
        self.scheduler = scheduler
        self.scan_workers = scan_workers
        self.checker_queue = checker_queue
        self.checker_scheduler = checker_scheduler
        self.enabled = enabled
        self.console = Console(force_terminal=force_terminal, file=sys.stdout)
        self.status_provider = status_provider
        self.scan_controller = scan_controller
        self.running = False

        self.progress_bar_column = BarColumn(
            bar_width=40,
            style="grey23",
            complete_style="bright_red",
            finished_style="bright_green",
            pulse_style="red",
        )
        self.progress_bar = RichProgress(
            TextColumn("[bold cyan]Transfer[/bold cyan]"),
            self.progress_bar_column,
            TextColumn("[bold white]{task.fields[progress_pct]}[/bold white]"),
            TextColumn("[bright_white]{task.fields[progress_detail]}[/bright_white]"),
            TextColumn("[bright_white]{task.fields[speed_detail]}[/bright_white]"),
            TextColumn("[bright_black]{task.fields[eta_detail]}[/bright_black]"),
            console=self.console,
            expand=False,
        )
        self.progress_task_id = self.progress_bar.add_task(
            "upload",
            total=1,
            completed=0,
            progress_pct="0.0%",
            progress_detail="0B/0B",
            speed_detail="0.0B/s",
            eta_detail="--:--:--",
        )

    # ================================
    # 启动仪表盘
    # ================================
    def start(self):
        self.running = True

    # ================================
    # 停止仪表盘
    # ================================
    def stop(self):
        self.running = False

    # ================================
    # 构建指标表格
    # ================================
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
        scan_active_workers = snapshot["scan_active_workers"]

        upload_snapshot = (
            self.scheduler.get_status_snapshot()
            if hasattr(self.scheduler, "get_status_snapshot")
            else {}
        )
        active_workers = int(upload_snapshot.get("active_workers", self.scheduler.get_active_workers()))
        stalled_workers = int(upload_snapshot.get("stalled_workers", 0) or 0)

        checker_active_workers = 0
        checker_stalled_workers = 0
        if self.checker_scheduler is not None:
            checker_snapshot = (
                self.checker_scheduler.get_status_snapshot()
                if hasattr(self.checker_scheduler, "get_status_snapshot")
                else {}
            )
            checker_active_workers = int(
                checker_snapshot.get("active_workers", self.checker_scheduler.get_active_workers())
            )
            checker_stalled_workers = int(checker_snapshot.get("stalled_workers", 0) or 0)

        status = self.status_provider() if self.status_provider is not None else {}
        index_status = status.get("index", "unknown")
        raw_scan_status = status.get("scan", "unknown")
        raw_check_status = status.get("check", "unknown")

        scan_status = raw_scan_status
        scan_worker_display = str(self.scan_workers)
        target_scan_workers = None
        if self.scan_controller is not None:
            target_scan_workers = self.scan_controller.get_desired_workers()
            scan_worker_display = f"{target_scan_workers}/{self.scan_workers}"

        if raw_scan_status == "running":
            if target_scan_workers is not None:
                scan_status = f"running ({scan_active_workers} active, target {target_scan_workers})"
            else:
                scan_status = f"running ({scan_active_workers} active)"

        check_status = raw_check_status
        if self.checker_scheduler is not None:
            if checker_active_workers > 0:
                suffix = f"{checker_active_workers} active"
                if checker_stalled_workers > 0:
                    suffix += f", {checker_stalled_workers} stalled"
                check_status = f"running ({suffix})"
            elif self.checker_queue is not None and self.checker_queue.unfinished_tasks > 0:
                check_status = "queued"
            elif raw_scan_status in {"pending", "running"}:
                check_status = "waiting for scan"
            elif raw_check_status in {"done", "n/a"}:
                check_status = raw_check_status
            else:
                check_status = "done"

        if active_workers > 0:
            suffix = f"{active_workers} active"
            if stalled_workers > 0:
                suffix += f", {stalled_workers} stalled"
            upload_status = f"running ({suffix})"
        elif self.task_queue.unfinished_tasks > 0:
            upload_status = "queued"
        elif self.checker_scheduler is not None and (
            check_status not in {"done", "n/a", "idle"}
            or checker_active_workers > 0
            or (self.checker_queue is not None and self.checker_queue.unfinished_tasks > 0)
        ):
            parts = []
            if checker_active_workers > 0:
                parts.append(f"{checker_active_workers} checking")
            if self.checker_queue is not None and self.checker_queue.unfinished_tasks > 0:
                parts.append(f"{self.checker_queue.unfinished_tasks} queued")
            upload_status = (
                f"waiting for check ({', '.join(parts)})" if parts else "waiting for check"
            )
        elif raw_scan_status in {"pending", "running"}:
            upload_status = "waiting for scan"
        elif raw_scan_status in {"done", "n/a"} and (
            self.checker_scheduler is None or raw_check_status in {"done", "n/a"}
        ):
            upload_status = "done"
        else:
            upload_status = "idle"

        hit_rate = (cache_hit / cache_total * 100.0) if cache_total > 0 else 0.0
        scan_elapsed = max(time.time() - snapshot["scan_start"], 0.001)
        scan_speed = scan_files / scan_elapsed
        elapsed = max(time.time() - snapshot["start_time"], 0.001)
        upload_speed = done / elapsed / 1024 / 1024

        queue_current = self.task_queue.qsize()
        queue_max = getattr(self.task_queue, "maxsize", 0)
        queue_display = str(queue_current) if queue_max <= 0 else f"{queue_current}/{queue_max}"

        checker_queue_display = ""
        if self.checker_queue is not None:
            checker_current = self.checker_queue.qsize()
            checker_max = getattr(self.checker_queue, "maxsize", 0)
            checker_queue_display = (
                str(checker_current)
                if checker_max <= 0
                else f"{checker_current}/{checker_max}"
            )

        table = Table(
            box=box.SIMPLE_HEAVY,
            header_style="bold bright_white",
            border_style="bright_blue",
            row_styles=["none", "on grey11"],
            expand=False,
            pad_edge=False,
        )
        table.add_column("Metric", style="bold cyan", no_wrap=True)
        table.add_column("Value", style="bright_white")

        table.add_row("Files Done", str(files_done))
        table.add_row("Upload Skip", str(files_skip))
        table.add_row("Scan Skip", str(scan_skip))
        table.add_row("Index Status", self.render_status(index_status))
        table.add_row("Scan Status", self.render_status(scan_status))
        if self.checker_scheduler is not None:
            table.add_row("Check Status", self.render_status(check_status))
        table.add_row("Upload Status", self.render_status(upload_status))
        table.add_row("Progress", f"{done / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB")
        table.add_row("Cache Hit", f"{cache_hit}/{cache_total}")
        table.add_row("Hit Rate", f"{hit_rate:.1f}%")
        table.add_row("Scan Files", str(scan_files))
        table.add_row("Scan Speed", f"{scan_speed:.0f} files/s")
        table.add_row("Scan Errors", str(scan_errors))
        table.add_row("Upload Errors", str(upload_errors))
        table.add_row("Upload Speed", f"{upload_speed:.1f} MB/s")
        if self.checker_queue is not None:
            table.add_row("Check Queue", checker_queue_display)
        table.add_row("Transfer Queue", queue_display)
        if self.checker_scheduler is not None:
            table.add_row("Check Workers", str(len(self.checker_scheduler.threads)))
        table.add_row("Upload Workers", str(len(self.scheduler.threads)))
        table.add_row("Scan Workers", scan_worker_display)

        return table

    # ================================
    # 渲染状态颜色
    # ================================
    def render_status(self, value):
        text = str(value)
        lowered = text.lower()

        if "error" in lowered:
            style = "bold bright_red"
        elif lowered.startswith("running"):
            style = "bold yellow"
        elif lowered.startswith("done"):
            style = "bold bright_green"
        elif (
            lowered == "queued"
            or lowered.startswith("waiting for scan")
            or lowered.startswith("waiting for check")
        ):
            style = "bold magenta"
        elif lowered == "pending":
            style = "bold cyan"
        else:
            style = "bright_white"

        return Text(text, style=style)

    # ================================
    # 格式化字节大小
    # ================================
    @staticmethod
    def format_bytes(value):
        size = float(max(value or 0, 0))
        units = ["B", "KB", "MB", "GB", "TB", "PB"]

        for unit in units:
            if size < 1024 or unit == units[-1]:
                return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
            size /= 1024

        return "0B"

    # ================================
    # 格式化进度百分比
    # ================================
    @staticmethod
    def format_progress_pct(done, total):
        total_for_ratio = max(float(total or 0), float(done or 0), 1.0)
        percent = max(0.0, min(float(done or 0) / total_for_ratio * 100.0, 100.0))
        if percent < 1:
            return f"{percent:.2f}%"
        if percent < 10:
            return f"{percent:.1f}%"
        return f"{percent:.0f}%"

    # ================================
    # 格式化 ETA
    # ================================
    @staticmethod
    def format_eta(seconds):
        if seconds is None or seconds < 0:
            return "--:--:--"

        total_seconds = int(seconds)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours >= 100:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    # ================================
    # 构建进度条
    # ================================
    def build_progress_renderable(self, bar_width=None):
        snapshot = self.progress.snapshot()
        total = max(int(snapshot["total_bytes"] or 0), 0)
        done = max(int(snapshot["done_bytes"] or 0), 0)
        total_for_render = max(total, done, 1)
        elapsed = max(time.time() - snapshot["start_time"], 0.001)
        speed = done / elapsed if done > 0 else 0.0
        remaining = max(total_for_render - done, 0)
        eta_seconds = (remaining / speed) if speed > 0 else None

        if bar_width is not None:
            self.progress_bar_column.bar_width = max(20, min(40, int(bar_width)))

        self.progress_bar.update(
            self.progress_task_id,
            total=total_for_render,
            completed=min(done, total_for_render),
            progress_pct=self.format_progress_pct(done, total_for_render),
            progress_detail=f"{self.format_bytes(done)}/{self.format_bytes(total_for_render)}",
            speed_detail=f"{self.format_bytes(speed)}/s",
            eta_detail=self.format_eta(eta_seconds),
        )
        return self.progress_bar

    # ================================
    # 测量渲染宽度
    # ================================
    def measure_renderable_width(self, renderable):
        measurement = Measurement.get(self.console, self.console.options, renderable)
        return max(20, measurement.maximum)

    # ================================
    # 构建整体渲染对象
    # ================================
    def build_renderable(self):
        table = self.build_table()
        table_width = self.measure_renderable_width(table)
        content = Group(
            self.build_progress_renderable(bar_width=max(20, table_width - 26)),
            table,
        )
        return Panel(
            content,
            title="[bold bright_cyan]OBS Migration Dashboard[/bold bright_cyan]",
            border_style="bright_blue",
            padding=(0, 1),
            expand=False,
        )

    # ================================
    # 持续刷新直到结束
    # ================================
    def run_until(self, done_fn, poll_interval=0.2, start_fn=None):
        self.running = True

        if not self.enabled:
            if start_fn is not None:
                start_fn()
            while self.running and not done_fn():
                time.sleep(poll_interval)
            return

        with Live(
            self.build_renderable(),
            refresh_per_second=max(1, int(1 / poll_interval)),
            console=self.console,
            transient=False,
        ) as live:
            live.update(self.build_renderable(), refresh=True)

            if start_fn is not None:
                start_fn()

            while self.running:
                live.update(self.build_renderable(), refresh=True)
                sys.stdout.flush()

                if done_fn():
                    break

                time.sleep(poll_interval)
