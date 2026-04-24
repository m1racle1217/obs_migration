# -*- coding: utf-8 -*-
"""根据队列压力动态调节扫描并发度。"""

import logging
import threading
import time


# ================================
# 动态调节扫描并发
# ================================
class AdaptiveScanController:
    """根据任务队列占用情况动态调整有效扫描线程数。"""

    # ================================
    # 初始化扫描控制器
    # ================================
    def __init__(self, task_queue, max_workers, min_workers=1, sample_interval=0.5):
        self.task_queue = task_queue
        self.max_workers = max(1, int(max_workers or 1))
        self.min_workers = max(1, min(self.max_workers, int(min_workers or 1)))
        self.sample_interval = max(0.0, float(sample_interval or 0.0))

        self._desired_workers = self.max_workers
        self._granted_slots = 0
        self._last_refresh = 0.0
        self._stopped = False
        self._condition = threading.Condition()

    # ================================
    # 获取执行槽位
    # ================================
    def acquire_slot(self, cancel_event=None):
        with self._condition:
            while True:
                if self._stopped or (cancel_event is not None and cancel_event.is_set()):
                    return False

                self._refresh_unlocked()
                if self._granted_slots < self._desired_workers:
                    self._granted_slots += 1
                    return True

                self._condition.wait(timeout=max(self.sample_interval, 0.2))

    # ================================
    # 释放执行槽位
    # ================================
    def release_slot(self):
        with self._condition:
            if self._granted_slots > 0:
                self._granted_slots -= 1

            self._refresh_unlocked(force=True)
            self._condition.notify_all()

    # ================================
    # 停止控制器
    # ================================
    def stop(self):
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    # ================================
    # 获取目标线程数
    # ================================
    def get_desired_workers(self):
        with self._condition:
            self._refresh_unlocked()
            return self._desired_workers

    # ================================
    # 获取控制器快照
    # ================================
    def snapshot(self):
        with self._condition:
            self._refresh_unlocked()
            return {
                "desired_workers": self._desired_workers,
                "granted_slots": self._granted_slots,
                "max_workers": self.max_workers,
                "min_workers": self.min_workers,
            }

    # ================================
    # 刷新控制状态
    # ================================
    def _refresh_unlocked(self, force=False):
        now = time.time()
        if not force and self.sample_interval > 0 and (now - self._last_refresh) < self.sample_interval:
            return

        desired = self._compute_desired_workers_unlocked()
        if desired != self._desired_workers:
            logging.debug(
                "[SCAN_ADAPT] queue=%s/%s desired=%s",
                self.task_queue.qsize(),
                getattr(self.task_queue, "maxsize", 0),
                desired,
            )
            self._desired_workers = desired

        self._last_refresh = now

    # ================================
    # 计算目标线程数
    # ================================
    def _compute_desired_workers_unlocked(self):
        queue_maxsize = getattr(self.task_queue, "maxsize", 0)
        if queue_maxsize <= 0 or self.max_workers <= self.min_workers:
            return self.max_workers

        fill_ratio = self.task_queue.qsize() / float(queue_maxsize)
        fill_ratio = max(0.0, min(fill_ratio, 1.0))

        span = self.max_workers - self.min_workers
        desired = self.min_workers + int(round((1.0 - fill_ratio) * span))
        return max(self.min_workers, min(self.max_workers, desired))
