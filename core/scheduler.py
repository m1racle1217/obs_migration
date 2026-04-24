# core/scheduler.py
# -*- coding: utf-8 -*-
"""消费任务队列并并发调度上传任务。"""

import logging
import queue
import threading


# ================================
# 上传调度器
# ================================
class Scheduler:
    """维护上传线程池，并把队列任务分发给上传器。"""

    # ================================
    # 初始化调度器
    # ================================
    def __init__(self, task_queue, uploader, workers=32):

        self.task_queue = task_queue
        self.uploader = uploader
        self.workers = workers

        self.threads = []
        self.lock = threading.Lock()
        self.active_workers = 0
        self.running = True

    # ================================
    # 启动工作线程
    # ================================
    def start(self):

        logging.info("启动 worker: %s", self.workers)

        for i in range(self.workers):
            thread = threading.Thread(
                target=self._worker,
                name=f"Worker-{i:02d}",
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)

    # ================================
    # 工作线程主循环
    # ================================
    def _worker(self):

        while True:
            try:
                task = self.task_queue.get(timeout=1)
            except queue.Empty:
                if not self.running:
                    break
                continue

            try:
                with self.lock:
                    self.active_workers += 1

                self.uploader.upload(task)
            except Exception as exc:
                logging.error("worker error %s", exc)
            finally:
                with self.lock:
                    self.active_workers -= 1

                self.task_queue.task_done()

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
