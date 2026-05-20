# -*- coding: utf-8 -*-
"""Background task lifecycle controls for the Web console."""

import copy
import threading
import time
import traceback


class TaskControls:
    """Thread-safe pause/stop signals plus dashboard status storage."""

    def __init__(self):
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()
        self._status = {}
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

    def snapshot(self):
        with self._lock:
            return copy.deepcopy(self._status)


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
