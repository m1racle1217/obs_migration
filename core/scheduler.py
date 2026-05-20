# -*- coding: utf-8 -*-
"""消费任务队列并为不同阶段提供统一的 worker 调度能力。"""

import logging
import queue
import threading
import time


# ================================
# 通用任务调度器
# ================================
class Scheduler:
    """维护工作线程池，并为处理器提供心跳与卡死快照。"""

    # ================================
    # 初始化调度器
    # ================================
    def __init__(
        self,
        task_queue,
        handler,
        workers=32,
        stage_name="worker",
        stall_timeout=300,
        controls=None,
    ):
        self.task_queue = task_queue
        self.handler = handler
        self.workers = max(1, int(workers or 1))
        self.stage_name = str(stage_name or "worker")
        self.stall_timeout = max(float(stall_timeout or 0), 1.0)
        self.controls = controls

        self.threads = []
        self.lock = threading.Lock()
        self.active_workers = 0
        self.running = True
        self.worker_states = {}

    # ================================
    # 启动工作线程
    # ================================
    def start(self):
        logging.info("[SCHEDULER] start stage=%s workers=%s", self.stage_name, self.workers)

        for index in range(self.workers):
            thread = threading.Thread(
                target=self._worker,
                name=f"{self.stage_name.capitalize()}-{index:02d}",
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)

    # ================================
    # 工作线程主循环
    # ================================
    def _worker(self):
        worker_name = threading.current_thread().name

        while True:
            if not self._wait_until_claim_allowed():
                break

            try:
                task = self.task_queue.get(timeout=1)
            except queue.Empty:
                if not self.running:
                    break
                continue

            if not self._claim_still_allowed(task):
                continue

            try:
                with self.lock:
                    self.active_workers += 1
                    self.worker_states[worker_name] = self._build_worker_state(task)

                self._dispatch_task(task, worker_name)
            except Exception as exc:
                logging.exception("[SCHEDULER][%s] worker error: %s", self.stage_name, exc)
            finally:
                with self.lock:
                    self.active_workers = max(self.active_workers - 1, 0)
                    self.worker_states.pop(worker_name, None)

                self.task_queue.task_done()

    # ================================
    # 绛夊緟鎺у埗淇″彿鍏佽棰嗗彇浠诲姟
    # ================================
    def _wait_until_claim_allowed(self):
        if self.controls is None:
            return self.running

        while self.running:
            if self.controls.stop_requested():
                return False
            if not self.controls.pause_requested():
                return True
            self.controls.wait_if_paused(
                poll_interval=0.05,
                should_continue=lambda: self.running,
            )

        return False

    # ================================
    # 棰嗗彇鍚庡啀娆℃牎楠屾槸鍚﹀厑璁稿垎鍙?
    # ================================
    def _claim_still_allowed(self, task):
        if self.controls is None:
            return True

        if self.controls.stop_requested():
            self.task_queue.task_done()
            return False

        if not self.controls.pause_requested():
            return True

        self.controls.wait_if_paused(
            poll_interval=0.05,
            should_continue=lambda: self.running,
        )
        if not self.running or self.controls.stop_requested():
            self.task_queue.task_done()
            return False
        return True

    # ================================
    # 分发任务给处理器
    # ================================
    def _dispatch_task(self, task, worker_name):
        heartbeat = lambda detail=None: self.heartbeat(worker_name, detail=detail)

        if hasattr(self.handler, "process"):
            self.handler.process(task, heartbeat=heartbeat, worker_name=worker_name)
            return

        if hasattr(self.handler, "upload"):
            self.handler.upload(task, heartbeat=heartbeat, worker_name=worker_name)
            return

        raise TypeError("scheduler handler must provide process() or upload()")

    # ================================
    # 构建 worker 状态
    # ================================
    def _build_worker_state(self, task):
        now = time.time()
        summary = (
            task.get("source_display")
            or task.get("source_path")
            or task.get("relative_path")
            or task.get("source_key")
            or task.get("local")
            or ""
        )
        return {
            "task_summary": str(summary),
            "detail": "",
            "started_at": now,
            "last_heartbeat": now,
        }

    # ================================
    # 更新 worker 心跳
    # ================================
    def heartbeat(self, worker_name, detail=None):
        with self.lock:
            state = self.worker_states.get(worker_name)
            if state is None:
                return

            state["last_heartbeat"] = time.time()
            if detail is not None:
                state["detail"] = str(detail)

    # ================================
    # 停止调度器
    # ================================
    def stop(self):
        self.running = False
        for thread in self.threads:
            thread.join()

    # ================================
    # 获取活跃线程数
    # ================================
    def get_active_workers(self):
        with self.lock:
            return self.active_workers

    # ================================
    # 获取状态快照
    # ================================
    def get_status_snapshot(self):
        now = time.time()
        with self.lock:
            workers = []
            stalled = 0

            for worker_name, state in self.worker_states.items():
                item = dict(state)
                item["worker_name"] = worker_name
                item["stall_seconds"] = max(now - float(state.get("last_heartbeat", now)), 0.0)
                item["is_stalled"] = item["stall_seconds"] >= self.stall_timeout
                if item["is_stalled"]:
                    stalled += 1
                workers.append(item)

            return {
                "active_workers": self.active_workers,
                "stalled_workers": stalled,
                "workers": workers,
                "stage_name": self.stage_name,
            }
