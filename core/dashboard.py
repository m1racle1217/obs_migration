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


TEXT = {
    "zh": {
        "transfer": "传输 / Transfer",
        "panel_title": "OBS 迁移仪表盘 / OBS Migration Dashboard",
        "metric_column": "指标 / Metric",
        "value_column": "数值 / Value",
        "files_done": "完成文件 / Files Done",
        "upload_skip": "上传跳过 / Upload Skip",
        "scan_skip": "扫描跳过 / Scan Skip",
        "index_status": "索引状态 / Index Status",
        "scan_status": "扫描状态 / Scan Status",
        "check_status": "检查状态 / Check Status",
        "upload_status": "上传状态 / Upload Status",
        "progress": "进度 / Progress",
        "cache_hit": "缓存命中 / Cache Hit",
        "hit_rate": "命中率 / Hit Rate",
        "scan_files": "扫描文件 / Scan Files",
        "scan_speed": "扫描速度 / Scan Speed",
        "scan_errors": "扫描错误 / Scan Errors",
        "upload_errors": "上传错误 / Upload Errors",
        "process_speed": "累计处理速度 / Process Speed",
        "net_upload_speed": "实时上传速度 / Net Upload Speed",
        "check_queue": "检查队列 / Check Queue",
        "transfer_queue": "传输队列 / Transfer Queue",
        "check_workers": "检查线程 / Check Workers",
        "upload_workers": "上传线程 / Upload Workers",
        "scan_workers": "扫描线程 / Scan Workers",
        "status_pending": "待开始",
        "status_running": "运行中",
        "status_done": "完成",
        "status_na": "不适用",
        "status_unknown": "未知",
        "status_queued": "排队中",
        "status_wait_scan": "等待扫描",
        "status_wait_check": "等待检查",
        "status_idle": "空闲",
        "status_active": "活跃",
        "status_stalled": "卡住",
        "status_target": "目标",
        "status_checking": "检查中",
        "files_per_sec": "文件/秒",
        "progress_speed_prefix": "处理",
    },
    "en": {
        "transfer": "Transfer",
        "panel_title": "OBS Migration Dashboard",
        "metric_column": "Metric",
        "value_column": "Value",
        "files_done": "Files Done",
        "upload_skip": "Upload Skip",
        "scan_skip": "Scan Skip",
        "index_status": "Index Status",
        "scan_status": "Scan Status",
        "check_status": "Check Status",
        "upload_status": "Upload Status",
        "progress": "Progress",
        "cache_hit": "Cache Hit",
        "hit_rate": "Hit Rate",
        "scan_files": "Scan Files",
        "scan_speed": "Scan Speed",
        "scan_errors": "Scan Errors",
        "upload_errors": "Upload Errors",
        "process_speed": "Process Speed",
        "net_upload_speed": "Net Upload Speed",
        "check_queue": "Check Queue",
        "transfer_queue": "Transfer Queue",
        "check_workers": "Check Workers",
        "upload_workers": "Upload Workers",
        "scan_workers": "Scan Workers",
        "status_pending": "pending",
        "status_running": "running",
        "status_done": "done",
        "status_na": "n/a",
        "status_unknown": "unknown",
        "status_queued": "queued",
        "status_wait_scan": "waiting for scan",
        "status_wait_check": "waiting for check",
        "status_idle": "idle",
        "status_active": "active",
        "status_stalled": "stalled",
        "status_target": "target",
        "status_checking": "checking",
        "files_per_sec": "files/s",
        "progress_speed_prefix": "Proc",
    },
}


DASHBOARD_STYLE = {
    "panel_border": "color(39)",
    "panel_title": "bold color(45)",
    "progress_label": "bold color(81)",
    "progress_complete": "color(82)",
    "progress_finished": "bold color(118)",
    "progress_pulse": "color(213)",
    "progress_percent": "bold color(226)",
    "progress_detail": "color(159)",
    "progress_speed": "bold color(118)",
    "progress_eta": "color(245)",
    "table_border": "color(33)",
    "table_header": "bold color(231)",
    "metric": "bold color(117)",
    "value": "color(255)",
    "count": "bold color(220)",
    "speed": "bold color(118)",
    "queue_ok": "color(159)",
    "queue_busy": "bold color(220)",
    "queue_full": "bold color(203)",
    "error": "bold color(203)",
    "status_running": "bold color(226)",
    "status_done": "bold color(82)",
    "status_wait": "bold color(213)",
    "status_pending": "bold color(81)",
    "muted": "color(245)",
}


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
        language="zh",
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
        self.language = self.normalize_language(language)

        self.progress_bar_column = BarColumn(
            bar_width=40,
            style="grey23",
            complete_style=DASHBOARD_STYLE["progress_complete"],
            finished_style=DASHBOARD_STYLE["progress_finished"],
            pulse_style=DASHBOARD_STYLE["progress_pulse"],
        )
        self.progress_bar = RichProgress(
            TextColumn(f"[{DASHBOARD_STYLE['progress_label']}]{self.t('transfer')}[/]"),
            self.progress_bar_column,
            TextColumn(f"[{DASHBOARD_STYLE['progress_percent']}]{{task.fields[progress_pct]}}[/]"),
            TextColumn(f"[{DASHBOARD_STYLE['progress_detail']}]{{task.fields[progress_detail]}}[/]"),
            TextColumn(f"[{DASHBOARD_STYLE['progress_speed']}]{{task.fields[speed_detail]}}[/]"),
            TextColumn(f"[{DASHBOARD_STYLE['progress_eta']}]{{task.fields[eta_detail]}}[/]"),
            console=self.console,
            expand=False,
        )
        self.progress_task_id = self.progress_bar.add_task(
            "upload",
            total=1,
            completed=0,
            progress_pct="0.0%",
            progress_detail="0B/0B",
            speed_detail=f"{self.t('progress_speed_prefix')} 0.0B/s",
            eta_detail="--:--:--",
        )

    # ================================
    # 规范化语言
    # ================================
    @staticmethod
    def normalize_language(language):
        text = str(language or "zh").strip().lower()
        return "en" if text in {"en", "english"} else "zh"

    # ================================
    # 获取多语言文本
    # ================================
    def t(self, key):
        return TEXT.get(self.language, TEXT["zh"]).get(key, key)

    # ================================
    # 本地化基础状态
    # ================================
    def format_base_status(self, raw_status):
        mapping = {
            "pending": self.t("status_pending"),
            "running": self.t("status_running"),
            "done": self.t("status_done"),
            "n/a": self.t("status_na"),
            "unknown": self.t("status_unknown"),
            "queued": self.t("status_queued"),
            "waiting for scan": self.t("status_wait_scan"),
            "waiting for check": self.t("status_wait_check"),
            "idle": self.t("status_idle"),
        }
        return mapping.get(str(raw_status or "").strip().lower(), str(raw_status))

    # ================================
    # 格式化运行中状态
    # ================================
    def format_running_status(self, active, stalled=None, target=None):
        if self.language == "zh":
            parts = [f"{active} {self.t('status_active')}"]
            if stalled:
                parts.append(f"{stalled} {self.t('status_stalled')}")
            if target is not None:
                parts.append(f"{self.t('status_target')} {target}")
            return f"{self.t('status_running')}（{'，'.join(parts)}）"

        parts = [f"{active} {self.t('status_active')}"]
        if stalled:
            parts.append(f"{stalled} {self.t('status_stalled')}")
        if target is not None:
            parts.append(f"{self.t('status_target')} {target}")
        return f"{self.t('status_running')} ({', '.join(parts)})"

    # ================================
    # 格式化等待检查状态
    # ================================
    def format_wait_check_status(self, checking=None, queued=None):
        parts = []
        if checking:
            parts.append(
                f"{checking} {self.t('status_checking')}"
                if self.language == "en"
                else f"{checking} {self.t('status_checking')}"
            )
        if queued:
            parts.append(f"{queued} {self.t('status_queued')}")

        if not parts:
            return self.t("status_wait_check")

        if self.language == "zh":
            return f"{self.t('status_wait_check')}（{'，'.join(parts)}）"
        return f"{self.t('status_wait_check')} ({', '.join(parts)})"

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
        recent_upload_bytes = snapshot.get("recent_upload_bytes", 0)
        recent_upload_window = max(snapshot.get("recent_upload_window", 5.0), 0.001)

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
        index_status_raw = status.get("index", "unknown")
        scan_status_raw = status.get("scan", "unknown")
        check_status_raw = status.get("check", "unknown")

        scan_worker_display = str(self.scan_workers)
        target_scan_workers = None
        if self.scan_controller is not None:
            target_scan_workers = self.scan_controller.get_desired_workers()
            scan_worker_display = f"{target_scan_workers}/{self.scan_workers}"

        if scan_status_raw == "running":
            scan_status = self.format_running_status(
                scan_active_workers,
                target=target_scan_workers,
            )
            scan_style_hint = "running"
        else:
            scan_status = self.format_base_status(scan_status_raw)
            scan_style_hint = scan_status_raw

        if self.checker_scheduler is not None:
            if checker_active_workers > 0:
                check_status = self.format_running_status(
                    checker_active_workers,
                    stalled=checker_stalled_workers,
                )
                check_style_hint = "running"
            elif self.checker_queue is not None and self.checker_queue.unfinished_tasks > 0:
                check_status = self.format_base_status("queued")
                check_style_hint = "queued"
            elif scan_status_raw in {"pending", "running"}:
                check_status = self.format_base_status("waiting for scan")
                check_style_hint = "waiting for scan"
            elif check_status_raw in {"done", "n/a"}:
                check_status = self.format_base_status(check_status_raw)
                check_style_hint = check_status_raw
            else:
                check_status = self.format_base_status("done")
                check_style_hint = "done"
        else:
            check_status = self.format_base_status(check_status_raw)
            check_style_hint = check_status_raw

        if active_workers > 0:
            upload_status = self.format_running_status(
                active_workers,
                stalled=stalled_workers,
            )
            upload_style_hint = "running"
        elif self.task_queue.unfinished_tasks > 0:
            upload_status = self.format_base_status("queued")
            upload_style_hint = "queued"
        elif self.checker_scheduler is not None and (
            check_style_hint not in {"done", "n/a", "idle"}
            or checker_active_workers > 0
            or (self.checker_queue is not None and self.checker_queue.unfinished_tasks > 0)
        ):
            queued_count = self.checker_queue.unfinished_tasks if self.checker_queue is not None else 0
            upload_status = self.format_wait_check_status(
                checking=checker_active_workers,
                queued=queued_count,
            )
            upload_style_hint = "waiting for check"
        elif scan_status_raw in {"pending", "running"}:
            upload_status = self.format_base_status("waiting for scan")
            upload_style_hint = "waiting for scan"
        elif scan_status_raw in {"done", "n/a"} and (
            self.checker_scheduler is None or check_status_raw in {"done", "n/a"}
        ):
            upload_status = self.format_base_status("done")
            upload_style_hint = "done"
        else:
            upload_status = self.format_base_status("idle")
            upload_style_hint = "idle"

        hit_rate = (cache_hit / cache_total * 100.0) if cache_total > 0 else 0.0
        scan_elapsed = max(time.time() - snapshot["scan_start"], 0.001)
        scan_speed = scan_files / scan_elapsed
        elapsed = max(time.time() - snapshot["start_time"], 0.001)
        process_speed = done / elapsed
        net_upload_speed = recent_upload_bytes / recent_upload_window

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
            header_style=DASHBOARD_STYLE["table_header"],
            border_style=DASHBOARD_STYLE["table_border"],
            row_styles=["none", "on grey7"],
            expand=False,
            pad_edge=False,
        )
        table.add_column(self.t("metric_column"), style=DASHBOARD_STYLE["metric"], no_wrap=True)
        table.add_column(self.t("value_column"), style=DASHBOARD_STYLE["value"])

        table.add_row(self.render_metric("files_done"), self.render_value(files_done, "count"))
        table.add_row(self.render_metric("upload_skip"), self.render_value(files_skip, "muted_count"))
        table.add_row(self.render_metric("scan_skip"), self.render_value(scan_skip, "muted_count"))
        table.add_row(
            self.render_metric("index_status"),
            self.render_status(self.format_base_status(index_status_raw), index_status_raw),
        )
        table.add_row(self.render_metric("scan_status"), self.render_status(scan_status, scan_style_hint))
        if self.checker_scheduler is not None:
            table.add_row(self.render_metric("check_status"), self.render_status(check_status, check_style_hint))
        table.add_row(self.render_metric("upload_status"), self.render_status(upload_status, upload_style_hint))
        table.add_row(
            self.render_metric("progress"),
            self.render_value(f"{done / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB", "progress"),
        )
        table.add_row(self.render_metric("cache_hit"), self.render_value(f"{cache_hit}/{cache_total}", "ratio"))
        table.add_row(self.render_metric("hit_rate"), self.render_value(f"{hit_rate:.1f}%", "ratio"))
        table.add_row(self.render_metric("scan_files"), self.render_value(scan_files, "count"))
        table.add_row(self.render_metric("scan_speed"), self.render_value(f"{scan_speed:.0f} {self.t('files_per_sec')}", "speed"))
        table.add_row(self.render_metric("scan_errors"), self.render_value(scan_errors, "error_count"))
        table.add_row(self.render_metric("upload_errors"), self.render_value(upload_errors, "error_count"))
        table.add_row(self.render_metric("process_speed"), self.render_value(f"{self.format_bytes(process_speed)}/s", "speed"))
        table.add_row(self.render_metric("net_upload_speed"), self.render_value(f"{self.format_bytes(net_upload_speed)}/s", "speed"))
        if self.checker_queue is not None:
            table.add_row(self.render_metric("check_queue"), self.render_queue(checker_queue_display))
        table.add_row(self.render_metric("transfer_queue"), self.render_queue(queue_display))
        if self.checker_scheduler is not None:
            table.add_row(self.render_metric("check_workers"), self.render_value(len(self.checker_scheduler.threads), "workers"))
        table.add_row(self.render_metric("upload_workers"), self.render_value(len(self.scheduler.threads), "workers"))
        table.add_row(self.render_metric("scan_workers"), self.render_value(scan_worker_display, "workers"))

        return table

    # ================================
    # 渲染指标名称颜色
    # ================================
    def render_metric(self, key):
        style = DASHBOARD_STYLE["metric"]
        if key.endswith("_status"):
            style = "bold color(81)"
        elif "queue" in key:
            style = "bold color(159)"
        elif "error" in key:
            style = "bold color(203)"
        elif "speed" in key:
            style = "bold color(118)"
        return Text(self.t(key), style=style)

    # ================================
    # 渲染指标值颜色
    # ================================
    def render_value(self, value, kind="default"):
        text = str(value)
        style = DASHBOARD_STYLE["value"]
        if kind in {"count", "workers"}:
            style = DASHBOARD_STYLE["count"]
        elif kind == "muted_count":
            style = DASHBOARD_STYLE["muted"] if str(value) in {"0", ""} else DASHBOARD_STYLE["count"]
        elif kind == "error_count":
            style = DASHBOARD_STYLE["muted"] if str(value) in {"0", ""} else DASHBOARD_STYLE["error"]
        elif kind in {"speed", "ratio"}:
            style = DASHBOARD_STYLE["speed"]
        elif kind == "progress":
            style = DASHBOARD_STYLE["progress_detail"]
        return Text(text, style=style)

    # ================================
    # 渲染队列压力颜色
    # ================================
    def render_queue(self, value):
        text = str(value)
        style = DASHBOARD_STYLE["queue_ok"]
        if "/" in text:
            try:
                current, total = text.split("/", 1)
                ratio = float(current) / max(float(total), 1.0)
                if ratio >= 0.9:
                    style = DASHBOARD_STYLE["queue_full"]
                elif ratio >= 0.5:
                    style = DASHBOARD_STYLE["queue_busy"]
            except Exception:
                pass
        return Text(text, style=style)

    # ================================
    # 渲染状态颜色
    # ================================
    def render_status(self, value, style_hint=None):
        text = str(value)
        lowered = str(style_hint or value).lower()

        if "error" in lowered:
            style = DASHBOARD_STYLE["error"]
        elif lowered.startswith("running"):
            style = DASHBOARD_STYLE["status_running"]
        elif lowered.startswith("done"):
            style = DASHBOARD_STYLE["status_done"]
        elif (
            lowered == "queued"
            or lowered.startswith("waiting for scan")
            or lowered.startswith("waiting for check")
        ):
            style = DASHBOARD_STYLE["status_wait"]
        elif lowered == "pending":
            style = DASHBOARD_STYLE["status_pending"]
        else:
            style = DASHBOARD_STYLE["value"]

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
        process_speed = done / elapsed if done > 0 else 0.0
        remaining = max(total_for_render - done, 0)
        eta_seconds = (remaining / process_speed) if process_speed > 0 else None

        if bar_width is not None:
            self.progress_bar_column.bar_width = max(20, min(40, int(bar_width)))

        self.progress_bar.update(
            self.progress_task_id,
            total=total_for_render,
            completed=min(done, total_for_render),
            progress_pct=self.format_progress_pct(done, total_for_render),
            progress_detail=f"{self.format_bytes(done)}/{self.format_bytes(total_for_render)}",
            speed_detail=f"{self.t('progress_speed_prefix')} {self.format_bytes(process_speed)}/s",
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
            title=f"[{DASHBOARD_STYLE['panel_title']}]{self.t('panel_title')}[/]",
            border_style=DASHBOARD_STYLE["panel_border"],
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
