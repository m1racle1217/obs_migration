# -*- coding: utf-8 -*-
"""Background task lifecycle controls for the Web console."""

import copy
import configparser
import threading
import time
import traceback
import uuid


class TaskControls:
    """Thread-safe pause/stop signals plus dashboard status storage."""

    def __init__(self):
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()
        self._status = {}
        self._concurrency = {}
        self._lock = threading.Lock()

    def pause_requested(self):
        return self.pause_event.is_set()

    def stop_requested(self):
        return self.stop_event.is_set()

    def wait_if_paused(self, poll_interval=0.1, should_continue=None):
        while self.pause_requested() and not self.stop_requested():
            if should_continue is not None and not should_continue():
                break
            time.sleep(max(float(poll_interval or 0.1), 0.001))

    def update_status(self, **values):
        with self._lock:
            self._status.update(values)

    def update_concurrency(self, **values):
        clean = {}
        for key, value in values.items():
            if value is None:
                continue
            try:
                clean[key] = max(1, int(value))
            except (TypeError, ValueError):
                continue
        with self._lock:
            self._concurrency.update(clean)

    def get_concurrency(self):
        with self._lock:
            return dict(self._concurrency)

    def snapshot(self):
        with self._lock:
            data = copy.deepcopy(self._status)
            data["concurrency"] = copy.deepcopy(self._concurrency)
            return data


class TaskManager:
    """Runs one migration task at a time in a daemon thread."""

    STATES = {
        "idle",
        "starting",
        "running",
        "pausing",
        "paused",
        "stopping",
        "stopped",
        "failed",
        "completed",
    }
    ACTIVE_STATES = {"starting", "running", "pausing", "paused", "stopping"}

    def __init__(self, runner):
        self.runner = runner
        self.controls = None
        self._thread = None
        self._state = "idle"
        self._error = None
        self._timestamps = {
            "created_at": time.time(),
            "started_at": None,
            "finished_at": None,
        }
        self._lock = threading.Lock()

    def start(self, cfg):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            if self._state in self.ACTIVE_STATES:
                return False

            self.controls = TaskControls()
            self._state = "starting"
            self._error = None
            self._timestamps = {
                "created_at": self._timestamps["created_at"],
                "started_at": time.time(),
                "finished_at": None,
            }
            self._thread = threading.Thread(
                target=self._run,
                args=(cfg, self.controls),
                daemon=True,
                name="TaskManager",
            )
            self._thread.start()
            return True

    def _run(self, cfg, controls):
        self._mark_started(controls)
        try:
            self.runner(cfg, controls)
        except Exception:
            with self._lock:
                self._state = "failed"
                self._error = traceback.format_exc()
                self._timestamps["finished_at"] = time.time()
            return

        with self._lock:
            self._state = "stopped" if controls.stop_requested() else "completed"
            self._timestamps["finished_at"] = time.time()

    def pause(self):
        with self._lock:
            if self.controls is None or self._state not in {"starting", "running", "pausing", "paused"}:
                return False
            self.controls.pause_event.set()
            self._state = "paused"
            return True

    def resume(self):
        with self._lock:
            if self.controls is None or self._state not in {"pausing", "paused", "running"}:
                return False
            self.controls.pause_event.clear()
            if self._thread is not None and self._thread.is_alive():
                self._state = "running"
            return True

    def stop(self):
        with self._lock:
            if self.controls is None:
                self._state = "stopped"
                self._timestamps["finished_at"] = self._timestamps["finished_at"] or time.time()
                return False
            self.controls.stop_event.set()
            if self._thread is not None and self._thread.is_alive():
                self._state = "stopping"
            elif self._state not in {"failed", "completed"}:
                self._state = "stopped"
            return True

    def join(self, timeout=None):
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def snapshot(self):
        with self._lock:
            controls = self.controls
            state = self._state
            error = self._error
            timestamps = dict(self._timestamps)

        controls_snapshot = controls.snapshot() if controls is not None else {}
        return {
            "state": state,
            "progress": controls_snapshot.get("progress", {}),
            "pipeline": controls_snapshot.get("pipeline", {}),
            "workers": controls_snapshot.get("workers", {}),
            "queues": controls_snapshot.get("queues", {}),
            "concurrency": controls_snapshot.get("concurrency", {}),
            "logs": controls_snapshot.get("logs", {}),
            "error": error,
            "timestamps": timestamps,
        }

    def _set_state(self, state):
        with self._lock:
            if state not in self.STATES:
                raise ValueError(f"invalid task state: {state}")
            self._state = state

    def _mark_started(self, controls):
        with self._lock:
            if controls.pause_requested() or self._state in {"pausing", "paused"}:
                self._state = "paused"
                return
            self._state = "running"


class ManagedMigrationTask:
    """One independently runnable Web migration task."""

    STATES = TaskManager.STATES
    ACTIVE_STATES = TaskManager.ACTIVE_STATES

    def __init__(self, task_id, name, cfg, runner):
        self.task_id = task_id
        self.name = name or task_id
        self.cfg = _copy_config(cfg)
        self.runner = runner
        self.controls = None
        self._thread = None
        self._state = "idle"
        self._error = None
        self._timestamps = {
            "created_at": time.time(),
            "started_at": None,
            "finished_at": None,
        }
        self._lock = threading.Lock()
        self._concurrency = _extract_concurrency(self.cfg)

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            if self._state in self.ACTIVE_STATES:
                return False

            self.controls = TaskControls()
            self.controls.update_concurrency(**self._concurrency)
            self._state = "starting"
            self._error = None
            self._timestamps["started_at"] = time.time()
            self._timestamps["finished_at"] = None
            self._thread = threading.Thread(
                target=self._run,
                args=(self.controls,),
                daemon=True,
                name=f"MigrationTask-{self.task_id}",
            )
            self._thread.start()
            return True

    def _run(self, controls):
        self._mark_started(controls)
        try:
            self.runner(_copy_config(self.cfg), controls)
        except Exception:
            with self._lock:
                self._state = "failed"
                self._error = traceback.format_exc()
                self._timestamps["finished_at"] = time.time()
            return

        with self._lock:
            self._state = "stopped" if controls.stop_requested() else "completed"
            self._timestamps["finished_at"] = time.time()

    def pause(self):
        with self._lock:
            if self.controls is None or self._state not in {"starting", "running", "pausing", "paused"}:
                return False
            self.controls.pause_event.set()
            self._state = "paused"
            return True

    def resume(self):
        with self._lock:
            if self.controls is None or self._state not in {"pausing", "paused", "running"}:
                return False
            self.controls.pause_event.clear()
            if self._thread is not None and self._thread.is_alive():
                self._state = "running"
            return True

    def stop(self):
        with self._lock:
            if self.controls is None:
                self._state = "stopped"
                self._timestamps["finished_at"] = self._timestamps["finished_at"] or time.time()
                return False
            self.controls.stop_event.set()
            if self._thread is not None and self._thread.is_alive():
                self._state = "stopping"
            elif self._state not in {"failed", "completed"}:
                self._state = "stopped"
            return True

    def join(self, timeout=None):
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def update_concurrency(self, values):
        clean = _normalize_concurrency(values)
        if not clean:
            return self.snapshot()

        with self._lock:
            self._concurrency.update(clean)
            _apply_concurrency_to_config(self.cfg, clean)
            controls = self.controls
        if controls is not None:
            controls.update_concurrency(**clean)
        return self.snapshot()

    def config_copy(self):
        with self._lock:
            return _copy_config(self.cfg)

    def summary(self):
        snapshot = self.snapshot()
        return {
            "task_id": snapshot["task_id"],
            "name": snapshot["name"],
            "state": snapshot["state"],
            "source": _source_summary(self.cfg),
            "target": _target_summary(self.cfg),
            "progress": snapshot["progress"],
            "dashboard": snapshot["dashboard"],
            "concurrency": snapshot["concurrency"],
            "logs": snapshot["logs"],
            "error": snapshot["error"],
            "timestamps": snapshot["timestamps"],
        }

    def snapshot(self):
        with self._lock:
            controls = self.controls
            state = self._state
            error = self._error
            timestamps = dict(self._timestamps)
            concurrency = dict(self._concurrency)

        controls_snapshot = controls.snapshot() if controls is not None else {}
        if controls_snapshot.get("concurrency"):
            concurrency.update(controls_snapshot.get("concurrency", {}))
        progress = controls_snapshot.get("progress", {})
        pipeline = controls_snapshot.get("pipeline", {})
        workers = controls_snapshot.get("workers", {})
        queues = controls_snapshot.get("queues", {})
        logs = controls_snapshot.get("logs", {})
        dashboard = _build_dashboard(progress, pipeline, workers, queues)
        return {
            "task_id": self.task_id,
            "name": self.name,
            "state": state,
            "source": _source_summary(self.cfg),
            "target": _target_summary(self.cfg),
            "progress": progress,
            "pipeline": pipeline,
            "workers": workers,
            "queues": queues,
            "dashboard": dashboard,
            "concurrency": concurrency,
            "logs": logs,
            "error": error,
            "timestamps": timestamps,
        }

    def _mark_started(self, controls):
        with self._lock:
            if controls.pause_requested() or self._state in {"pausing", "paused"}:
                self._state = "paused"
                return
            self._state = "running"


class MultiTaskManager:
    """Manages multiple independently runnable migration tasks."""

    def __init__(self, runner):
        self.runner = runner
        self._tasks = {}
        self._selected_task_id = None
        self._lock = threading.Lock()

    def create_task(self, cfg, name=None, task_id=None):
        task_id = task_id or uuid.uuid4().hex[:12]
        task = ManagedMigrationTask(task_id, name or f"任务 {len(self._tasks) + 1}", cfg, self.runner)
        with self._lock:
            while task_id in self._tasks:
                task_id = uuid.uuid4().hex[:12]
                task.task_id = task_id
            self._tasks[task_id] = task
            self._selected_task_id = task_id
        return task_id

    def list_tasks(self):
        with self._lock:
            tasks = list(self._tasks.values())
        return [task.summary() for task in tasks]

    def get_task_config(self, task_id):
        return self._task(task_id).config_copy()

    def snapshot(self, task_id=None):
        task = self._task_or_default(task_id)
        if task is None:
            return _empty_task_snapshot()
        return task.snapshot()

    def start(self, task_id_or_cfg=None):
        if _looks_like_config(task_id_or_cfg):
            task_id = self.create_task(task_id_or_cfg)
        else:
            task_id = task_id_or_cfg
        task = self._task_or_default(task_id)
        if task is None:
            return False
        return task.start()

    def pause(self, task_id=None):
        task = self._task_or_default(task_id)
        return False if task is None else task.pause()

    def resume(self, task_id=None):
        task = self._task_or_default(task_id)
        return False if task is None else task.resume()

    def stop(self, task_id=None):
        if task_id is None:
            task = self._task_or_default(None)
            return False if task is None else task.stop()
        return self._task(task_id).stop()

    def stop_all(self):
        with self._lock:
            tasks = list(self._tasks.values())
        result = False
        for task in tasks:
            result = task.stop() or result
        return result

    def join(self, task_id, timeout=None):
        return self._task(task_id).join(timeout=timeout)

    def join_all(self, timeout=None):
        with self._lock:
            tasks = list(self._tasks.values())
        return all(task.join(timeout=timeout) for task in tasks)

    def update_concurrency(self, task_id, values):
        return self._task(task_id).update_concurrency(values)

    def select_task(self, task_id):
        with self._lock:
            if task_id not in self._tasks:
                raise KeyError(task_id)
            self._selected_task_id = task_id

    def _task_or_default(self, task_id):
        with self._lock:
            if task_id is None:
                task_id = self._selected_task_id or next(iter(self._tasks), None)
            if task_id is None:
                return None
            return self._tasks.get(task_id)

    def _task(self, task_id):
        task = self._task_or_default(task_id)
        if task is None:
            raise KeyError(task_id)
        return task


def _looks_like_config(value):
    return hasattr(value, "sections") and hasattr(value, "get")


def _copy_config(cfg):
    copied = configparser.ConfigParser()
    for section in cfg.sections():
        copied.add_section(section)
        for key, value in cfg[section].items():
            copied.set(section, key, value)
    return copied


def _normalize_concurrency(values):
    key_map = {
        "upload_workers": ("UPLOAD", "workers"),
        "check_workers": ("UPLOAD", "checkers"),
        "scan_workers": ("SCAN", "scan_workers"),
        "multipart_concurrency": ("UPLOAD", "multipart_concurrency"),
        "max_connections": ("UPLOAD", "max_connections"),
    }
    clean = {}
    for key in key_map:
        if key not in values:
            continue
        try:
            clean[key] = max(1, int(values[key]))
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be an integer")
    return clean


def _extract_concurrency(cfg):
    return {
        "upload_workers": _cfg_int(cfg, "UPLOAD", "workers", 1),
        "check_workers": _cfg_int(cfg, "UPLOAD", "checkers", max(1, _cfg_int(cfg, "UPLOAD", "workers", 1) // 2)),
        "scan_workers": _cfg_int(cfg, "SCAN", "scan_workers", 1),
        "multipart_concurrency": _cfg_int(cfg, "UPLOAD", "multipart_concurrency", 1),
        "max_connections": _cfg_int(cfg, "UPLOAD", "max_connections", 1),
    }


def _apply_concurrency_to_config(cfg, values):
    mapping = {
        "upload_workers": ("UPLOAD", "workers"),
        "check_workers": ("UPLOAD", "checkers"),
        "scan_workers": ("SCAN", "scan_workers"),
        "multipart_concurrency": ("UPLOAD", "multipart_concurrency"),
        "max_connections": ("UPLOAD", "max_connections"),
    }
    for key, value in values.items():
        section, option = mapping[key]
        if not cfg.has_section(section):
            cfg.add_section(section)
        cfg.set(section, option, str(value))


def _cfg_int(cfg, section, option, default):
    try:
        return max(1, int(cfg.get(section, option, fallback=str(default)) or default))
    except (TypeError, ValueError):
        return max(1, int(default or 1))


def _source_summary(cfg):
    if not cfg.has_section("SOURCE"):
        return ""
    source_type = cfg.get("SOURCE", "type", fallback="")
    if source_type == "local":
        return cfg.get("SOURCE", "path", fallback="")
    bucket = cfg.get("SOURCE", "bucket", fallback="")
    prefix = cfg.get("SOURCE", "prefix", fallback="")
    return "/".join(item for item in (bucket, prefix) if item)


def _target_summary(cfg):
    if not cfg.has_section("TARGET"):
        return ""
    target_type = cfg.get("TARGET", "type", fallback="")
    if target_type == "local":
        return cfg.get("TARGET", "path", fallback="")
    bucket = cfg.get("TARGET", "bucket", fallback="")
    prefix = cfg.get("TARGET", "prefix", fallback="")
    return "/".join(item for item in (bucket, prefix) if item)


def _build_dashboard(progress, pipeline, workers, queues):
    progress = progress or {}
    pipeline = pipeline or {}
    workers = workers or {}
    queues = queues or {}
    done = max(int(progress.get("done_bytes", 0) or 0), 0)
    total = max(int(progress.get("total_bytes", 0) or 0), 0)
    total_for_ratio = max(total, done, 1)
    elapsed = max(time.time() - float(progress.get("start_time", time.time()) or time.time()), 0.001)
    scan_elapsed = max(time.time() - float(progress.get("scan_start", time.time()) or time.time()), 0.001)
    process_speed = done / elapsed
    recent_upload_window = max(float(progress.get("recent_upload_window", 5.0) or 5.0), 0.001)
    net_upload_speed = max(int(progress.get("recent_upload_bytes", 0) or 0), 0) / recent_upload_window
    remaining = max(total_for_ratio - done, 0)
    eta_seconds = remaining / process_speed if process_speed > 0 else None
    cache_hit = int(progress.get("cache_hit", 0) or 0)
    cache_total = int(progress.get("cache_total", 0) or 0)
    hit_rate = cache_hit / cache_total * 100.0 if cache_total > 0 else 0.0
    scan_files = int(progress.get("scan_files", 0) or 0)
    return {
        "percent": done / total_for_ratio * 100.0,
        "done_bytes": done,
        "total_bytes": total,
        "eta_seconds": eta_seconds,
        "files_done": int(progress.get("files_done", 0) or 0),
        "upload_skip": int(progress.get("files_skip", 0) or 0),
        "scan_skip": int(progress.get("scan_skip", 0) or 0),
        "index_status": pipeline.get("index", "unknown"),
        "scan_status": pipeline.get("scan", "unknown"),
        "check_status": pipeline.get("check", "unknown"),
        "upload_status": _stage_status(workers.get("upload", {}), queues.get("transfer", {})),
        "cache_hit": cache_hit,
        "cache_total": cache_total,
        "hit_rate": hit_rate,
        "scan_files": scan_files,
        "scan_speed": scan_files / scan_elapsed,
        "scan_errors": int(progress.get("scan_errors", 0) or 0),
        "upload_errors": int(progress.get("upload_errors", 0) or 0),
        "process_speed": process_speed,
        "net_upload_speed": net_upload_speed,
        "check_queue": queues.get("check", {}),
        "transfer_queue": queues.get("transfer", {}),
        "check_workers": workers.get("check", {}),
        "upload_workers": workers.get("upload", {}),
        "scan_workers": {
            "active_workers": int(progress.get("scan_active_workers", 0) or 0),
        },
        "active_workers": _active_workers(workers),
    }


def _stage_status(worker_snapshot, queue_snapshot):
    active = int((worker_snapshot or {}).get("active_workers", 0) or 0)
    queued = int((queue_snapshot or {}).get("current", 0) or 0)
    if active > 0:
        return "running"
    if queued > 0:
        return "queued"
    return "idle"


def _active_workers(workers):
    items = []
    for stage, snapshot in (workers or {}).items():
        for item in (snapshot or {}).get("workers", []) or []:
            worker = dict(item)
            worker["stage"] = stage
            items.append(worker)
    return items


def _empty_task_snapshot():
    return {
        "task_id": None,
        "name": "",
        "state": "idle",
        "source": "",
        "target": "",
        "progress": {},
        "pipeline": {},
        "workers": {},
        "queues": {},
        "dashboard": _build_dashboard({}, {}, {}, {}),
        "concurrency": {},
        "logs": {},
        "error": None,
        "timestamps": {"created_at": time.time(), "started_at": None, "finished_at": None},
    }
