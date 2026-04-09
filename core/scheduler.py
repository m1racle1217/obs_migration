# core/scheduler.py
# -*- coding: utf-8 -*-

import threading
import queue
import logging


class Scheduler:

    def __init__(self, task_queue, uploader, workers=32):

        self.task_queue = task_queue
        self.uploader = uploader
        self.workers = workers

        self.threads = []

        self.running = True

    # ===================================
    # 启动 worker
    # ===================================

    def start(self):

        logging.info(f"启动 worker: {self.workers}")

        for i in range(self.workers):

            t = threading.Thread(
                target=self._worker,
                name=f"Worker-{i:02d}",
                daemon=True
            )

            t.start()

            self.threads.append(t)

    # ===================================
    # worker
    # ===================================

    def _worker(self):

        while True:

            try:

                task = self.task_queue.get(timeout=1)

            except queue.Empty:

                if not self.running:
                    break

                continue

            try:

                self.uploader.upload(task)

            except Exception as e:

                logging.error(f"worker error {e}")

            finally:

                self.task_queue.task_done()

    # ===================================
    # 停止
    # ===================================

    def stop(self):

        self.running = False

        for t in self.threads:

            t.join()